"use strict";
/**
 * QQ message-element decoding, distilled from NapCat's element handling.
 *
 * QQNT messages carry an `elements` array. Each element is a tagged union
 * (`textElement`, `picElement`, `pttElement`, `arkElement`, ...). The chat UI
 * only needs a coarse kind plus a short displayable text, so this module maps
 * every known element to `{ kind, text, type }` and never fabricates text it did
 * not read.
 *
 * `kind` is one of: "text" | "image" | "other" | "system".
 */

const ELEMENT_KINDS = {
  textElement: { kind: "text" },
  picElement: { kind: "image", label: "[图片]" },
  pttElement: { kind: "other", label: "[语音]" },
  videoElement: { kind: "other", label: "[视频]" },
  fileElement: { kind: "other", label: "[文件]" },
  arkElement: { kind: "other", label: "[卡片]" },
  faceElement: { kind: "other", label: "[表情]" },
  marketFaceElement: { kind: "other", label: "[表情]" },
  replyElement: { kind: "other", label: "[回复]" },
  multiMsgElement: { kind: "other", label: "[合并转发]" },
  walletElement: { kind: "other", label: "[红包]" },
  grayTipElement: { kind: "system" },
  giphyElement: { kind: "other", label: "[表情]" },
};

/** System/gray-tip element subtypes that should not become a chat row. */
function isSystemElement(element) {
  return Boolean(element && element.grayTipElement);
}

function textOf(element) {
  if (!element) {
    return "";
  }
  const text = element.textElement || element.text;
  if (typeof text === "string") {
    return text;
  }
  if (text && typeof text.content === "string") {
    return text.content;
  }
  return "";
}

function firstNonEmpty(...values) {
  for (const value of values) {
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return "";
}

/**
 * Reduce one message's elements to a display row.
 * Returns `null` for messages that must not be rendered (system tips).
 */
function decodeElements(elements) {
  const list = Array.isArray(elements) ? elements : [];
  const parts = [];
  let kind = "other";
  let type = "unknown";
  let imageElement = null;

  for (const element of list) {
    if (!element || typeof element !== "object") {
      continue;
    }
    const key = Object.keys(element).find((name) => ELEMENT_KINDS[name]);
    const spec = key ? ELEMENT_KINDS[key] : null;
    if (!spec) {
      continue;
    }
    if (spec.kind === "system") {
      if (list.length === 1) {
        return null;
      }
      continue;
    }
    type = key;
    if (key === "textElement" || spec.kind === "text") {
      const value = textOf(element);
      if (value) {
        parts.push(value);
        if (kind !== "image") {
          kind = "text";
        }
      }
      continue;
    }
    if (spec.kind === "image") {
      if (kind !== "text") {
        kind = "image";
      }
      if (imageElement === null) {
        imageElement = element.picElement || element;
      }
      parts.push(spec.label || "[图片]");
      continue;
    }
    if (kind !== "text" && kind !== "image") {
      kind = "other";
    }
    parts.push(spec.label || `[${type}]`);
  }

  const text = parts.join(" ").trim();
  if (!text) {
    return null;
  }
  return { kind, text, type, imageElement };
}

/** A short one-line conversation preview derived from decoded elements. */
function previewOf(elements) {
  const decoded = decodeElements(elements);
  if (!decoded) {
    return "[消息]";
  }
  return decoded.text.replace(/\s+/g, " ").slice(0, 120);
}

/** Locate the original picture descriptor inside a decoded image message. */
function pictureOf(decoded) {
  if (!decoded || decoded.kind !== "image" || !decoded.imageElement) {
    return null;
  }
  const picture = decoded.imageElement.picElement || decoded.imageElement;
  if (!picture || typeof picture !== "object") {
    return null;
  }
  return {
    sourcePath: firstNonEmpty(picture.sourcePath, picture.thumbPath, picture.originPath),
    fileName: firstNonEmpty(picture.fileName, picture.filename),
    md5: firstNonEmpty(picture.md5HexStr, picture.md5),
    fileUuid: firstNonEmpty(picture.fileUuid, picture.fileUUID),
    url: firstNonEmpty(picture.originImageUrl, picture.sourceUrl),
  };
}

module.exports = { decodeElements, previewOf, pictureOf, isSystemElement };