"use strict";
/**
 * Read-only QQNT chat operations built on the kernel services.
 *
 * Everything here only calls QQNT "get/list/query" style methods. It never sends,
 * recalls, mutates or uploads anything. The result shapes are the wire contract
 * consumed by `bridge/qq_client.py` and `bridge/qq_source.py`.
 *
 * Two ideas are taken from NapCat (an injected, in-process QQNT bot):
 *  1. the reader runs inside QQ's own Electron main process, so the native
 *     kernel services are reachable through a plain `require()`;
 *  2. messages are addressed by an opaque cursor returned by the kernel.
 * NapCat itself is not vendored or started; only these ideas are re-implemented.
 */
const crypto = require("crypto");
const fs = require("fs");
const { services, invoke, invokeAny } = require("./wrapper");
const { decodeElements, previewOf, pictureOf } = require("./elements");
const { log } = require("./paths");

const CHAT_BUDDY = 1;
const CHAT_GROUP = 2;
const MAX_IMAGE_BYTES = 8 * 1024 * 1024;
const DIRECTORY_TTL_MS = 30_000;
const SCAN_TTL_MS = 60_000;
// Bounded full-history index so a scan can never grow without limit.
const MAX_SCAN_MESSAGES = 50_000;
// Shard label kept in every sort key. QQNT messages have no shard files, so a
// single constant keeps the (time, shard, seq) ordering used by the backend.
const SHARD = "qq";

function toMs(value) {
  const number = Number(value);
  if (!Number.isFinite(number) || number <= 0) {
    return 0;
  }
  return number < 10_000_000_000 ? Math.round(number * 1000) : Math.round(number);
}

function toInt(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? Math.trunc(number) : fallback;
}

function pick(object, ...keys) {
  if (!object) {
    return undefined;
  }
  for (const key of keys) {
    if (object[key] !== undefined && object[key] !== null) {
      return object[key];
    }
  }
  return undefined;
}

function isGroupChat(chatType, peer) {
  if (toInt(chatType, 0) === CHAT_GROUP) {
    return true;
  }
  if (toInt(chatType, 0) === CHAT_BUDDY) {
    return false;
  }
  // A pure-numeric peer is a group code; QQNT buddies are uid strings.
  return typeof peer === "string" && /^\d+$/.test(peer);
}

function buddyAvatarUrl(uin) {
  return uin ? `https://thirdqq.qlogo.cn/g?b=qq&nk=${encodeURIComponent(uin)}&s=640` : "";
}

function groupAvatarUrl(groupCode) {
  return groupCode
    ? `https://p.qlogo.cn/gh/${encodeURIComponent(groupCode)}/${encodeURIComponent(groupCode)}/640`
    : "";
}

function sortOf(message) {
  return [message.msgTime, SHARD, message.msgSeq];
}

function compareSort(left, right) {
  const a = Array.isArray(left) ? left : [0, "", 0];
  const b = Array.isArray(right) ? right : [0, "", 0];
  const seqDelta = toInt(a[0]) - toInt(b[0]);
  if (seqDelta !== 0) {
    return seqDelta < 0 ? -1 : 1;
  }
  if (String(a[1]) !== String(b[1])) {
    return String(a[1]) < String(b[1]) ? -1 : 1;
  }
  const localDelta = toInt(a[2]) - toInt(b[2]);
  return localDelta === 0 ? 0 : localDelta < 0 ? -1 : 1;
}

function sniffMime(data) {
  if (data.length >= 3 && data[0] === 0xff && data[1] === 0xd8 && data[2] === 0xff) {
    return "image/jpeg";
  }
  if (data.length >= 8 && data.slice(0, 8).equals(Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]))) {
    return "image/png";
  }
  if (data.length >= 6 && (data.slice(0, 6).toString("ascii") === "GIF87a" || data.slice(0, 6).toString("ascii") === "GIF89a")) {
    return "image/gif";
  }
  if (data.length >= 12 && data.slice(0, 4).toString("ascii") === "RIFF" && data.slice(8, 12).toString("ascii") === "WEBP") {
    return "image/webp";
  }
  return null;
}

class QqntReader {
  constructor(options = {}) {
    this.account = null;
    this.selfUid = null;
    this.selfUin = "";
    this.nickname = "";
    this.directory = null;
    this.directoryAt = 0;
    this.uinByUid = new Map();
    this.nameByUid = new Map();
    this.scans = new Map();
    this.options = options;
  }

  /** Resolve the logged-in identity. Retries until QQ has finished login. */
  async selfInfo() {
    const misc = await services.misc();
    const profile = await services.profile();
    const info =
      (await invokeAny(
        [
          ["profile", profile],
          ["misc", misc],
        ],
        "getSelfInfo",
      ).catch(() => null)) || {};
    const uid = String(pick(info, "uid", "selfUid", "uin") || "");
    const uin = String(pick(info, "uin", "uinString") || "");
    const nickname = String(pick(info, "nick", "nickName", "nickname") || "");
    this.selfUid = uid || this.selfUid;
    this.selfUin = uin || this.selfUin;
    this.nickname = nickname || this.nickname;
    if (!this.account) {
      // The stable account scope is the QQ uid; uin is only used for display.
      this.account = uid || uin || null;
    }
    return {
      account: this.account,
      uid: this.selfUid,
      uin: this.selfUin,
      nickname: this.nickname,
      avatar: buddyAvatarUrl(this.selfUin),
      avatarCandidates: this.selfUin ? [buddyAvatarUrl(this.selfUin)] : [],
      ready: Boolean(this.account),
    };
  }

  /** Refresh the friend/group directory used for names and avatars. */
  async loadDirectory(force = false) {
    if (this.directory && !force && Date.now() - this.directoryAt < DIRECTORY_TTL_MS) {
      return this.directory;
    }
    const buddyService = await services.buddy();
    const groupService = await services.group();
    const friends =
      (await invokeAny([["buddy", buddyService]], "getBuddyListFromCache").catch(() => null)) || [];
    const groups =
      (await invokeAny(
        [
          ["group", groupService],
          ["buddy", buddyService],
        ],
        "getGroupListFromCache",
      ).catch(() => null)) ||
      (await invokeAny([["group", groupService]], "getGroupsFromCache").catch(() => null)) ||
      [];

    const friendList = (friends.buddyList || friends.list || friends || []).map((item) => ({
      uid: String(pick(item, "uid", "uin") || ""),
      uin: String(pick(item, "uin", "uinString") || ""),
      name: String(pick(item, "remark", "nick", "nickName") || ""),
    }));
    const groupList = (groups.groupList || groups.list || groups || []).map((item) => ({
      peerUid: String(pick(item, "groupCode", "groupUin", "peerUid") || ""),
      name: String(pick(item, "groupName", "name") || ""),
      memberCount: toInt(pick(item, "memberCount", "memberNum"), 0),
      remark: String(pick(item, "remark") || ""),
    }));

    this.uinByUid.clear();
    this.nameByUid.clear();
    for (const friend of friendList) {
      if (friend.uid) {
        this.nameByUid.set(friend.uid, friend.name || friend.uin);
        if (friend.uin) {
          this.uinByUid.set(friend.uid, friend.uin);
        }
      }
    }
    this.directory = { friends: friendList, groups: groupList };
    this.directoryAt = Date.now();
    return this.directory;
  }

  async groupMembers(groupCode) {
    const groupService = await services.group();
    const result =
      (await invokeAny(
        [["group", groupService]],
        "getAllMemberList",
        String(groupCode),
        true,
      ).catch(() => null)) ||
      (await invokeAny(
        [["group", groupService]],
        "getGroupMemberListFromCache",
        String(groupCode),
      ).catch(() => null)) ||
      {};
    const list = result.members || result.memberList || result || [];
    return list.map((member) => ({
      uid: String(pick(member, "uid", "memberUid") || ""),
      uin: String(pick(member, "uin", "uinString") || ""),
      name: String(pick(member, "cardName", "nick", "nickName", "remark") || ""),
    }));
  }

  async contacts() {
    if (!this.account) {
      await this.selfInfo().catch(() => null);
    }
    const directory = await this.loadDirectory(true);
    return {
      friends: directory.friends.map((friend) => ({
        id: friend.uid,
        name: friend.name || friend.uin,
        avatar: buddyAvatarUrl(friend.uin),
        avatarCandidates: friend.uin ? [buddyAvatarUrl(friend.uin)] : [],
      })),
      groups: directory.groups.map((group) => ({
        id: `${group.peerUid}@chatroom`,
        peer: group.peerUid,
        name: group.remark || group.name,
        avatar: groupAvatarUrl(group.peerUid),
        avatarCandidates: group.peerUid ? [groupAvatarUrl(group.peerUid)] : [],
        memberCount: group.memberCount,
      })),
    };
  }

  contactFor(peer) {
    if (!this.directory) {
      return null;
    }
    const groupCode = String(peer || "").replace(/@chatroom$/, "");
    const group = this.directory.groups.find((item) => item.peerUid === groupCode);
    if (group) {
      return {
        id: `${groupCode}@chatroom`,
        name: group.remark || group.name || groupCode,
        avatar: groupAvatarUrl(groupCode),
        avatarCandidates: groupCode ? [groupAvatarUrl(groupCode)] : [],
        isGroup: true,
      };
    }
    const friend = this.directory.friends.find((item) => item.uid === peer);
    if (friend) {
      return {
        id: peer,
        name: friend.name || friend.uin || peer,
        avatar: buddyAvatarUrl(friend.uin),
        avatarCandidates: friend.uin ? [buddyAvatarUrl(friend.uin)] : [],
        isGroup: false,
      };
    }
    return null;
  }

  async sessions() {
    if (!this.account) {
      await this.selfInfo().catch(() => null);
    }
    const recentService = await services.recent();
    const raw =
      (await invokeAny(
        [["recent", recentService]],
        "getRecentContactList",
        { count: 500 },
      ).catch(() => null)) ||
      (await invokeAny([["recent", recentService]], "getRecentContactListSync").catch(() => null)) ||
      {};
    await this.loadDirectory(false);
    const items = (raw.recentContactList || raw.contacts || raw || []).map((contact) => {
      const peer = String(pick(contact, "peerUid", "peerUin", "targetUid") || "");
      const chatType = toInt(pick(contact, "chatType"), isGroupChat(0, peer) ? CHAT_GROUP : CHAT_BUDDY);
      const group = isGroupChat(chatType, peer);
      const directoryContact = this.contactFor(peer);
      const name =
        (directoryContact && directoryContact.name) ||
        String(pick(contact, "peerName", "sendNickName", "sendMemberName") || peer);
      const lastMsgTime = toMs(pick(contact, "msgTime", "lastMsgTime"));
      const preview = String(pick(contact, "summery", "summary", "preview") || "").trim();
      return {
        // Group peers carry the backend's group suffix so the analysis layer can
        // recognise them without a QQ-specific branch.
        username: group ? `${peer}@chatroom` : peer,
        peer,
        name,
        avatar: directoryContact ? directoryContact.avatar : "",
        avatarCandidates: directoryContact ? directoryContact.avatarCandidates : [],
        preview: preview || "[消息]",
        time: lastMsgTime,
        sortTimestamp: lastMsgTime,
        unreadCount: toInt(pick(contact, "unreadCnt", "unread"), 0),
        pinned: Boolean(pick(contact, "isTop", "pinned")),
        lastSender: String(pick(contact, "sendMemberName", "sendNickName") || ""),
        isGroup: group,
      };
    });
    const self = {
      username: this.selfUid || this.selfUin,
      name: this.nickname || this.selfUin,
      avatar: buddyAvatarUrl(this.selfUin),
      avatarCandidates: this.selfUin ? [buddyAvatarUrl(this.selfUin)] : [],
    };
    return { self, sessions: items, account: this.account, messagesReady: true };
  }

  /** Fetch one newest-first kernel page of messages, tolerating API renames. */
  async _fetchPage(peer, chatType, count, cursor, isGroup) {
    const msgService = await services.msg();
    const size = Math.max(1, Math.min(toInt(count, 20), 200));
    const page =
      (await invokeAny(
        [["msg", msgService]],
        "getMsgsWithCursor",
        peer,
        size,
        cursor || "",
      ).catch(() => null)) ||
      (await invokeAny(
        [["msg", msgService]],
        "getMsgsIncludeSelf",
        peer,
        size,
        cursor || "",
        false,
      ).catch(() => null)) ||
      (await invokeAny(
        [["msg", msgService]],
        "getAioFirstViewLatestMsgs",
        peer,
        size,
      ).catch(() => null));
    if (!page) {
      return { list: [], cursor: "", chatType };
    }
    const list = page.msgList || page.messages || (Array.isArray(page) ? page : []);
    const next = page.cursor || page.nextCursor || page.nextMsgId || "";
    const detectedType = toInt(pick(page, "chatType"), isGroup ? CHAT_GROUP : chatType);
    return { list, cursor: String(next || ""), chatType: detectedType };
  }

  normalizeMessage(raw, peer, isGroup) {
    const chatType = toInt(pick(raw, "chatType"), isGroup ? CHAT_GROUP : CHAT_BUDDY);
    const group = isGroupChat(chatType, peer);
    const senderUid = String(pick(raw, "senderUid", "senderUin") || "");
    const sendType = toInt(pick(raw, "sendType"), -1);
    const decoded = decodeElements(raw.elements);
    if (!decoded) {
      return null;
    }
    const msgId = String(pick(raw, "msgId", "msgID", "id") || "");
    if (!msgId) {
      return null;
    }
    const msgTime = toMs(pick(raw, "msgTime", "time"));
    const msgSeq = toInt(pick(raw, "msgSeq", "msgseq"), 0);
    const senderName = String(
      pick(raw, "sendMemberName", "sendNickName", "senderName") ||
        this.nameByUid.get(senderUid) ||
        senderUid ||
        "",
    );
    const self = senderUid === this.selfUid || sendType === 1;
    const picture = pictureOf(decoded);
    const message = {
      msgId,
      peerUid: peer,
      msgTime,
      msgSeq,
      senderId: senderUid,
      senderName,
      avatar: buddyAvatarUrl(
        this.uinByUid.get(senderUid) || (senderUid === this.selfUid ? this.selfUin : ""),
      ),
      side: self ? "self" : "other",
      kind: decoded.kind,
      type: decoded.type,
      text: decoded.text,
      isGroup: group,
      image: picture ? this.imageRef(peer, msgId, picture) : null,
      sort: [msgTime, SHARD, msgSeq],
    };
    return message;
  }

  /** Newest-first fetch, bounded, de-duplicated, returned oldest-first. */
  async messages(peer, { order = "latest", anchor = null, limit = 80, offset = 0, isGroup = false } = {}) {
    const size = Math.max(1, Math.min(toInt(limit, 80), 500));
    const skip = Math.max(0, toInt(offset, 0));
    const needed = size + skip;
    const collected = [];
    let cursor = "";
    let guard = 0;
    while (collected.length < needed + 1 && guard < 80) {
      guard += 1;
      const page = await this._fetchPage(peer, isGroup ? CHAT_GROUP : CHAT_BUDDY, Math.min(200, needed + 20 - collected.length), cursor, isGroup);
      if (!page.list.length) {
        break;
      }
      cursor = page.cursor;
      for (const raw of page.list) {
        const message = this.normalizeMessage(raw, peer, isGroup);
        if (message && !collected.some((item) => item.msgId === message.msgId)) {
          collected.push(message);
        }
      }
      if (!cursor) {
        break;
      }
    }
    collected.sort((left, right) => compareSort(right.sort, left.sort));
    const hasMoreBefore = collected.length > needed;
    // Drop the newest `skip` rows, then take the next `size`, oldest-first.
    let selected = collected.slice(skip, skip + size);
    if (order === "before" && anchor) {
      selected = collected.filter((item) => compareSort(item.sort, anchor) < 0).slice(0, size);
    } else if (order === "after" && anchor) {
      selected = collected.filter((item) => compareSort(item.sort, anchor) > 0).slice(0, size);
    }
    selected = selected.slice().reverse();
    return { messages: selected, hasMoreBefore };
  }

  /** Build (or reuse) a bounded ascending index of one conversation. */
  async _scan(peer, isGroup) {
    const cached = this.scans.get(peer);
    if (cached && Date.now() - cached.at < SCAN_TTL_MS) {
      return cached;
    }
    const collected = [];
    const seen = new Set();
    let cursor = "";
    let guard = 0;
    let truncated = false;
    while (guard < 400) {
      guard += 1;
      const page = await this._fetchPage(peer, isGroup ? CHAT_GROUP : CHAT_BUDDY, 200, cursor, isGroup);
      if (!page.list.length) {
        break;
      }
      cursor = page.cursor;
      for (const raw of page.list) {
        const message = this.normalizeMessage(raw, peer, isGroup);
        if (message && !seen.has(message.msgId)) {
          seen.add(message.msgId);
          collected.push(message);
        }
      }
      if (collected.length >= MAX_SCAN_MESSAGES) {
        truncated = true;
        break;
      }
      if (!cursor) {
        break;
      }
    }
    collected.sort((left, right) => compareSort(left.sort, right.sort));
    const scan = { list: collected, at: Date.now(), truncated, isGroup };
    this.scans.set(peer, scan);
    log("scanned", peer, "messages:", collected.length, truncated ? "(truncated)" : "");
    return scan;
  }

  async historyHighwater(peer, isGroup) {
    const scan = await this._scan(peer, isGroup);
    if (!scan.list.length) {
      return null;
    }
    return scan.list[scan.list.length - 1].sort;
  }

  async historyPage(peer, { ceiling = null, after = null, limit = 1000, isGroup = false } = {}) {
    const pageSize = Math.max(1, Math.min(toInt(limit, 1000), 2000));
    const scan = await this._scan(peer, isGroup);
    let list = scan.list;
    if (ceiling) {
      list = list.filter((item) => compareSort(item.sort, ceiling) <= 0);
    }
    if (after) {
      list = list.filter((item) => compareSort(item.sort, after) > 0);
    }
    const page = list.slice(0, pageSize);
    if (!page.length) {
      return { messages: [], nextAfter: null };
    }
    return { messages: page, nextAfter: page[page.length - 1].sort };
  }

  async precededText(peer, before, limit = 3, isGroup = false) {
    const scan = await this._scan(peer, isGroup);
    const candidates = scan.list.filter(
      (item) => compareSort(item.sort, before) < 0 && item.kind === "text" && item.text.trim(),
    );
    return candidates.slice(Math.max(0, candidates.length - Math.max(1, toInt(limit, 3))));
  }

  /** The bounded window of messages strictly older than `before`, oldest first. */
  async historyBefore(peer, before, limit = 100, isGroup = false) {
    const size = Math.max(1, Math.min(toInt(limit, 100), 2000));
    const scan = await this._scan(peer, isGroup);
    const older = scan.list.filter((item) => compareSort(item.sort, before) < 0);
    return {
      messages: older.slice(Math.max(0, older.length - size)),
      hasMoreBefore: older.length > size,
    };
  }

  /** The bounded window of messages strictly newer than `after`, oldest first. */
  async historyAfter(peer, after, limit = 100, isGroup = false) {
    const size = Math.max(1, Math.min(toInt(limit, 100), 2000));
    const scan = await this._scan(peer, isGroup);
    const newer = scan.list.filter((item) => compareSort(item.sort, after) > 0);
    return { messages: newer.slice(0, size), hasMoreAfter: newer.length > size };
  }

  /** The single message occupying an exact sort position, or null. */
  async messageAt(peer, position, isGroup = false) {
    if (!Array.isArray(position)) {
      return null;
    }
    const scan = await this._scan(peer, isGroup);
    return scan.list.find((item) => compareSort(item.sort, position) === 0) || null;
  }

  /** Newest-first search over the cached conversation index. */
  async historySearch(peer, { before = null, startMs = null, endMs = null, query = "", limit = 50, isGroup = false } = {}) {
    const size = Math.max(1, Math.min(toInt(limit, 50), 500));
    const scan = await this._scan(peer, isGroup);
    let list = scan.list;
    if (before) {
      list = list.filter((item) => compareSort(item.sort, before) < 0);
    }
    if (startMs !== null && startMs !== undefined) {
      const start = toInt(startMs, 0);
      list = list.filter((item) => item.msgTime >= start);
    }
    if (endMs !== null && endMs !== undefined) {
      const end = toInt(endMs, 0);
      list = list.filter((item) => item.msgTime < end);
    }
    const needle = String(query || "").trim().toLowerCase();
    if (needle) {
      list = list.filter((item) => String(item.text || "").toLowerCase().includes(needle));
    }
    const matched = list.slice().reverse();
    const page = matched.slice(0, size);
    return {
      messages: page,
      hasMore: matched.length > size,
      nextAfter: page.length ? page[page.length - 1].sort : null,
    };
  }

  /** Distinct senders seen in one conversation, so the UI can name and avatar
   * every participant without a second kernel round-trip. */
  async senders(peer, isGroup = false) {
    const scan = await this._scan(peer, isGroup);
    const directory = new Map();
    for (const item of scan.list) {
      const id = item.senderId || "";
      if (!id || directory.has(id)) {
        continue;
      }
      directory.set(id, { id, name: item.senderName || id, avatar: item.avatar || "" });
    }
    return [...directory.values()];
  }

  /** Text bodies for the stable ids the analysis layer already persisted. */
  async textsByIds(peer, ids, isGroup = false) {
    const wanted = new Set(Array.isArray(ids) ? ids.map(String) : []);
    if (!wanted.size || !this.account) {
      return [];
    }
    const scan = await this._scan(peer, isGroup);
    const found = [];
    for (const item of scan.list) {
      if (item.kind !== "text" || !item.text) {
        continue;
      }
      const id = stableId(this.account, peer, item.msgId);
      if (wanted.has(id)) {
        found.push([id, item.text]);
      }
    }
    return found;
  }

  async quotedHistoryPage(peer, { ceiling = null, after = null, limit = 64, member = null, isGroup = false } = {}) {
    const pageSize = Math.max(1, Math.min(toInt(limit, 64), 512));
    const scan = await this._scan(peer, isGroup);
    let list = scan.list.filter((item) => item.type === "replyElement");
    if (member) {
      list = list.filter((item) => item.senderId === member);
    }
    if (ceiling) {
      list = list.filter((item) => compareSort(item.sort, ceiling) <= 0);
    }
    if (after) {
      list = list.filter((item) => compareSort(item.sort, after) > 0);
    }
    const page = list.slice(0, pageSize);
    if (!page.length) {
      return { messages: [], nextAfter: null };
    }
    return { messages: page, nextAfter: page[page.length - 1].sort };
  }

  async profileMetadata(peer, member, isGroup) {
    const scan = await this._scan(peer, isGroup);
    const counts = new Map();
    let count = 0;
    let textCount = 0;
    for (const item of scan.list) {
      count += 1;
      const sender = item.senderId || "";
      const previous = counts.get(sender) || { count: 0, text: 0 };
      previous.count += 1;
      const analyzable = item.kind === "text" && typeof item.text === "string" && item.text.trim();
      if (analyzable) {
        textCount += 1;
        previous.text += 1;
      }
      counts.set(sender, previous);
    }
    const subject = member || peer;
    const selected = counts.get(subject) || { count: 0, text: 0 };
    // Plain object so the JSON-RPC envelope carries the per-sender counts.
    return {
      counts: Object.fromEntries(counts),
      count: member ? selected.count : count,
      textCount: member ? selected.text : textCount,
      total: count,
    };
  }

  async groupMemberDirectory(peer) {
    const groupCode = String(peer || "").replace(/@chatroom$/, "");
    const members = await this.groupMembers(groupCode);
    const result = new Map();
    for (const member of members) {
      if (member.uid) {
        result.set(member.uid, member.name || member.uin || member.uid);
        if (member.uin) {
          this.uinByUid.set(member.uid, member.uin);
        }
      }
    }
    return result;
  }

  imageRef(peer, msgId, picture) {
    return {
      msgId,
      peerUid: peer,
      sourcePath: picture.sourcePath || "",
      fileName: picture.fileName || "",
      md5: picture.md5 || "",
      fileUuid: picture.fileUuid || "",
    };
  }

  /** Return base64 image bytes for one picture message, or an unavailability reason. */
  async imageData(ref) {
    let sourcePath = ref.sourcePath;
    if (!sourcePath || !fs.existsSync(sourcePath)) {
      const richMedia = await services.richMedia();
      const resolved =
        (await invokeAny(
          [["richMedia", richMedia]],
          "getImagePath",
          ref.peerUid,
          ref.msgId,
        ).catch(() => null)) ||
        (await invokeAny(
          [["richMedia", richMedia]],
          "getImageUrlOrPath",
          ref.peerUid,
          ref.msgId,
        ).catch(() => null));
      sourcePath = typeof resolved === "string" ? resolved : pick(resolved, "path", "filePath");
    }
    if (!sourcePath || !fs.existsSync(sourcePath)) {
      return { unavailable: "local-file-unavailable" };
    }
    let stat;
    try {
      stat = fs.statSync(sourcePath);
    } catch (_error) {
      return { unavailable: "local-file-unavailable" };
    }
    if (stat.size > MAX_IMAGE_BYTES) {
      return { unavailable: "image-too-large" };
    }
    let data;
    try {
      data = fs.readFileSync(sourcePath);
    } catch (_error) {
      return { unavailable: "decode-failed" };
    }
    const mime = sniffMime(data);
    if (!mime) {
      return { unavailable: "unsupported-format" };
    }
    return { data: data.toString("base64"), mime };
  }
}

function stableId(account, peer, msgId) {
  return crypto
    .createHash("sha256")
    .update(JSON.stringify([account || "", peer, String(msgId)]))
    .digest("hex");
}

module.exports = {
  QqntReader,
  isGroupChat,
  toMs,
  compareSort,
  sortOf,
  previewOf,
  stableId,
  SHARD,
  CHAT_BUDDY,
  CHAT_GROUP,
  log,
};