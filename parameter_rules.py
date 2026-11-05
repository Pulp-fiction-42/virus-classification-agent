"""Typed parameter patches. User text never becomes a shell argument."""

from __future__ import annotations

import copy
import re
import shlex
from dataclasses import dataclass
from typing import Any


class ParameterError(ValueError):
    pass


_CN_DIGITS = {
    "零": 0,
    "〇": 0,
    "一": 1,
    "二": 2,
    "两": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
}
_CN_UNITS = {"十": 10, "百": 100, "千": 1000}


def parse_integer(value: str) -> int:
    """Parse Arabic or common Chinese integer forms used in requests."""
    normalized = value.strip().replace(",", "").replace("，", "")
    if re.fullmatch(r"-?\d+", normalized):
        return int(normalized)
    if not normalized:
        raise ValueError("empty integer")
    sign = -1 if normalized.startswith("-") else 1
    if sign < 0:
        normalized = normalized[1:]
    if not normalized or any(char not in _CN_DIGITS and char not in _CN_UNITS for char in normalized):
        raise ValueError(f"unsupported integer: {value}")
    section = 0
    digit = 0
    for char in normalized:
        if char in _CN_DIGITS:
            digit = _CN_DIGITS[char]
            continue
        unit = _CN_UNITS[char]
        if digit == 0:
            digit = 1
        section += digit * unit
        digit = 0
    return sign * (section + digit)


@dataclass(frozen=True)
class ParameterRule:
    stage: str
    flag: str
    minimum: int | None = None
    maximum: int | None = None
    boolean: bool = False


RULES = {
    "fastp.length_required": ParameterRule("read_qc", "--length_required", 1, 1000),
    "fastp.qualified_quality_phred": ParameterRule("read_qc", "--qualified_quality_phred", 0, 40),
    "fastp.average_qual": ParameterRule("read_qc", "--average_qual", 0, 40),
    "fastp.n_base_limit": ParameterRule("read_qc", "--n_base_limit", 0, 1000),
    "fastp.dedup": ParameterRule("read_qc", "--dedup", boolean=True),
    "megahit.min_contig_len": ParameterRule("assembly", "--min-contig-len", 200, 100000),
    "contig_filter.min_length": ParameterRule("contig_filter", "", 200, 100000),
}


def validate_patch(patch: Any) -> dict[str, int | bool]:
    if not isinstance(patch, dict):
        raise ParameterError("参数修改必须是键值对象。")
    result = {}
    for key, value in patch.items():
        rule = RULES.get(key)
        if rule is None:
            raise ParameterError(f"暂不支持修改参数 {key}；不能直接提交任意命令行参数。")
        if rule.boolean:
            if not isinstance(value, bool):
                raise ParameterError(f"{key} 必须是 true 或 false。")
        elif isinstance(value, bool) or not isinstance(value, int) or not rule.minimum <= value <= rule.maximum:
            raise ParameterError(f"{key} 必须是 {rule.minimum} 到 {rule.maximum} 的整数，收到 {value!r}。")
        result[key] = value
    return result


def apply_patch(current: dict, patch: dict) -> dict:
    validated = validate_patch(patch)
    updated = copy.deepcopy(current)
    updated.update(validated)
    return updated


def parse_parameter_patch(text: str, stages: set[str]) -> dict[str, int | bool]:
    patch = {}
    explicit = re.compile(r"(fastp\.[a-z_]+|megahit\.min_contig_len|contig_filter\.min_length)\s*(?:改为|改成|设为|设置为|=|：|:)\s*([^\s,，;；。]+)", re.I)
    for match in explicit.finditer(text):
        key, raw = match.group(1).lower(), match.group(2).lower()
        if raw in {"true", "false"}:
            value = raw == "true"
        else:
            try:
                value = parse_integer(raw)
            except ValueError as exc:
                raise ParameterError(f"{key} 的值必须是整数或布尔值。") from exc
        patch[key] = value

    patterns = [
        (r"(?:read[s]?|读段|质控|fastp)\s*(?:的)?[^，。;；]{0,18}?(?:最短|最小|最低)(?:序列)?长度[^\d\-零〇一二两三四五六七八九十百千]{0,12}(-?[\d零〇一二两三四五六七八九十百千]+)", "fastp.length_required"),
        (r"(?:组装|megahit)\s*(?:的)?[^，。;；]{0,16}?(?:最短|最小)(?:\s*contig\s*)?长度[^\d\-零〇一二两三四五六七八九十百千]{0,12}(-?[\d零〇一二两三四五六七八九十百千]+)", "megahit.min_contig_len"),
        (r"(?:contig\s*过滤|过滤\s*contig|contigs?)\s*(?:的)?[^，。;；]{0,14}?(?:最短|最小|至少)(?:长度)?[^\d\-零〇一二两三四五六七八九十百千]{0,12}(-?[\d零〇一二两三四五六七八九十百千]+)", "contig_filter.min_length"),
        (r"(?:平均质量|average_qual)[^\d\-零〇一二两三四五六七八九十百千]{0,12}(-?[\d零〇一二两三四五六七八九十百千]+)", "fastp.average_qual"),
        (r"(?:合格碱基质量|qualified_quality_phred|碱基质量阈值)[^\d\-零〇一二两三四五六七八九十百千]{0,12}(-?[\d零〇一二两三四五六七八九十百千]+)", "fastp.qualified_quality_phred"),
        (r"(?:最多\s*N|N\s*碱基上限|n_base_limit)[^\d\-零〇一二两三四五六七八九十百千]{0,12}(-?[\d零〇一二两三四五六七八九十百千]+)", "fastp.n_base_limit"),
    ]
    for pattern, key in patterns:
        match = re.search(pattern, text, re.I)
        if match:
            try:
                patch[key] = parse_integer(match.group(1))
            except ValueError as exc:
                raise ParameterError(f"{key} 的值必须是整数。") from exc
    short = re.search(
        r"(?:最短|最小)长度[^\d\-零〇一二两三四五六七八九十百千]{0,12}"
        r"(-?[\d零〇一二两三四五六七八九十百千]+)",
        text,
    )
    if short and not any(k.endswith(("length_required", "min_contig_len", "min_length")) for k in patch):
        candidates = [k for k in ["fastp.length_required", "megahit.min_contig_len", "contig_filter.min_length"] if RULES[k].stage in stages]
        if len(candidates) != 1:
            raise ParameterError("“最短长度”指 fastp 的 read 长度、MEGAHIT 的组装长度，还是 contig 过滤长度？请指定。")
        try:
            patch[candidates[0]] = parse_integer(short.group(1))
        except ValueError as exc:
            raise ParameterError("最短长度必须是整数。") from exc
    if re.search(r"(?:关闭|取消|不要|不做|禁用)\s*(?:fastp\s*)?去重", text):
        patch["fastp.dedup"] = False
    elif re.search(r"(?:开启|启用|需要|执行)\s*(?:fastp\s*)?去重", text):
        patch["fastp.dedup"] = True
    return validate_patch(patch)


def replace_flag(command: str, flag: str, value: int | bool) -> str:
    tokens = shlex.split(command)
    kept = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token == flag:
            index += 1
            if not isinstance(value, bool) and index < len(tokens) and not tokens[index].startswith("--"):
                index += 1
            continue
        if token.startswith(flag + "="):
            index += 1
            continue
        kept.append(token)
        index += 1
    if value is not False:
        kept.append(flag)
        if not isinstance(value, bool):
            kept.append(str(value))
    return shlex.join(kept)
