"""
*******************************************************************
  Copyright (c) 2026 Ian Craggs

  All rights reserved. This program and the accompanying materials
  are made available under the terms of the Eclipse Public License v2.0
  and Eclipse Distribution License v1.0 which accompany this distribution.

  The Eclipse Public License is available at
     https://www.eclipse.org/legal/epl-2.0/
  and the Eclipse Distribution License is available at
    http://www.eclipse.org/org/documents/edl-v10.php.

  AI Disclosure: This file was partly AI-generated. The AI-generated
  portions are made available under CC0-1.0 and not subject to the
  project's licence. The human contributor has reviewed and verified
  that the code is correct.
  
  SPDX-License-Identifier: EPL-2.0 and CC0-1.0

  Contributors:
     Ian Craggs - initial implementation and/or documentation
*******************************************************************
"""

"""
MQTT-SN 2.0 Protection Encapsulation — cryptographic verification (Section 3.17).

This module implements authentication-tag verification for all protection schemes
defined in the MQTT-SN 2.0 specification (Table 3-39):

  Authentication-only schemes (MAC over AAD; plaintext inner packet):
    0x00  HMAC-SHA256
    0x01  HMAC-SHA3_256
    0x02  CMAC-128  (AES-128/CMAC)
    0x03  CMAC-192  (AES-192/CMAC)
    0x04  CMAC-256  (AES-256/CMAC)

  AEAD schemes (encrypt-then-authenticate; ciphertext replaces inner packet):
    0x40  AES-CCM-64-128   (64-bit tag, 128-bit key)
    0x41  AES-CCM-64-192   (64-bit tag, 192-bit key)
    0x42  AES-CCM-64-256   (64-bit tag, 256-bit key)
    0x43  AES-CCM-128-128  (128-bit tag, 128-bit key)
    0x44  AES-CCM-128-192  (128-bit tag, 192-bit key)
    0x45  AES-CCM-128-256  (128-bit tag, 256-bit key)
    0x46  AES-GCM-128-128  (128-bit tag, 128-bit key)
    0x47  AES-GCM-128-192  (128-bit tag, 192-bit key)
    0x48  AES-GCM-128-256  (128-bit tag, 256-bit key)
    0x49  ChaCha20/Poly1305 (128-bit tag, 256-bit key)

Public API
----------
verify_protection(packet, key_store) -> bytes | None

wrap_protection(inner_packet_bytes, sender_id, scheme_code, key_store) -> ProtectionEncapsulations

    Verifies the AuthenticationTag in a ProtectionEncapsulations packet and,
    for AEAD schemes, decrypts the ProtectedMQTTSNPacket ciphertext.

    Parameters
    ----------
    packet    : MQTTSN2.ProtectionEncapsulations
                The already-unpacked Protection Encapsulation packet.
    key_store : KeyStore  (or any object implementing lookup_key(sender_id) -> bytes | None)
                Provides the shared key for a given 8-byte SenderIdentifier.

    Returns
    -------
    bytes   The plaintext bytes of the inner MQTT-SN packet on success.
    None    If verification fails (wrong tag, unknown sender, unsupported scheme).
            The caller should send DISCONNECT(Protection scheme invalid) and drop the client.

    Raises
    ------
    ProtectionError   Subclass of Exception; carries a human-readable reason string
                      and an optional spec conformance label.  Callers may catch this
                      to distinguish "bad packet" from a plain None return.

KeyStore
--------
The concrete KeyStore class provided here is a simple in-memory dict.
Callers can supply any object with a lookup_key(sender_id: bytes) -> bytes | None method.

    ks = KeyStore()
    ks.add_key(sender_id=b'\\x00'*8, key=bytes.fromhex('...'))
    plaintext = verify_protection(packet, ks)

Nonce / IV derivation
---------------------
Per §3.17.3 the nonce/IV for CCM, GCM and ChaCha20/Poly1305 is SHA-256 of all
packet fields from byte 1 up to (but not including) the ProtectedMQTTSNPacket
field, truncated to the leftmost N bits needed by the scheme:
  CCM  → 104 bits (13 bytes)  [MQTT-SN-3.17.3-2]
  GCM  →  96 bits (12 bytes)  [MQTT-SN-3.17.3-3]
  ChaCha20/Poly1305 → 96 bits (12 bytes)  [MQTT-SN-3.17.3-4]

The same prefix bytes are used as Additional Authenticated Data (AAD) for
AEAD schemes.

Authentication tag truncation
------------------------------
Per §3.17.2.3:
  AuthTagLen == 0x0  provider-defined length; we use the scheme's nominal size.
  AuthTagLen == 0x1  nominal tag size  [MQTT-SN-3.17.2.3-2]
  AuthTagLen >= 0x4  tag length = AuthTagLen × 2 bytes (bits: AuthTagLen × 16)
                     only valid for authentication-only schemes [MQTT-SN-3.17.2.3-5/6]
  0x2, 0x3           reserved; rejected at unpack time by ProtectionFlags.
"""

import hmac
import hashlib
import logging
import struct

from cryptography.hazmat.primitives.cmac import CMAC
from cryptography.hazmat.primitives.ciphers import algorithms
from cryptography.hazmat.primitives.ciphers.aead import AESGCM, AESCCM, ChaCha20Poly1305
from cryptography.exceptions import InvalidTag

logger = logging.getLogger('MQTT-SN broker')


# ---------------------------------------------------------------------------
# Scheme metadata table
# ---------------------------------------------------------------------------

# Scheme code → (name, auth_only, key_bytes, nominal_tag_bytes)
_SCHEMES = {
    0x00: ("HMAC-SHA256",          True,  None, 32),
    0x01: ("HMAC-SHA3_256",        True,  None, 32),
    0x02: ("CMAC-128",             True,  16,   16),
    0x03: ("CMAC-192",             True,  24,   16),
    0x04: ("CMAC-256",             True,  32,   16),
    0x40: ("AES-CCM-64-128",       False, 16,    8),
    0x41: ("AES-CCM-64-192",       False, 24,    8),
    0x42: ("AES-CCM-64-256",       False, 32,    8),
    0x43: ("AES-CCM-128-128",      False, 16,   16),
    0x44: ("AES-CCM-128-192",      False, 24,   16),
    0x45: ("AES-CCM-128-256",      False, 32,   16),
    0x46: ("AES-GCM-128-128",      False, 16,   16),
    0x47: ("AES-GCM-128-192",      False, 24,   16),
    0x48: ("AES-GCM-128-256",      False, 32,   16),
    0x49: ("ChaCha20/Poly1305",    False, 32,   16),
}


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class ProtectionError(Exception):
    """Raised when a Protection Encapsulation packet fails verification."""
    def __init__(self, reason, conformance_label=None):
        super().__init__(reason)
        self.reason = reason
        self.conformance_label = conformance_label

    def __str__(self):
        if self.conformance_label:
            return f"{self.conformance_label}: {self.reason}"
        return self.reason


# ---------------------------------------------------------------------------
# Key store
# ---------------------------------------------------------------------------

class KeyStore:
    """Simple in-memory mapping of SenderIdentifier (8 bytes) → shared key."""

    def __init__(self):
        self._keys = {}   # bytes -> bytes

    def add_key(self, sender_id: bytes, key: bytes):
        """Register a shared key for the given 8-byte sender identifier."""
        if len(sender_id) != 8:
            raise ValueError(f"SenderIdentifier must be 8 bytes, got {len(sender_id)}")
        self._keys[sender_id] = key

    def remove_key(self, sender_id: bytes):
        """Remove the key for a sender identifier, if present."""
        self._keys.pop(sender_id, None)

    def lookup_key(self, sender_id: bytes):
        """Return the shared key for sender_id, or None if not registered."""
        return self._keys.get(sender_id)

    def __len__(self):
        return len(self._keys)


# ---------------------------------------------------------------------------
# AAD / nonce prefix construction
# ---------------------------------------------------------------------------

def _build_aad_prefix(packet) -> bytes:
    """Return the bytes from the start of the Protection Encapsulation packet
    header up to (but not including) the ProtectedMQTTSNPacket field.

    These bytes serve as:
      - the HMAC/CMAC message (authentication-only schemes authenticate
        the prefix concatenated with the ProtectedMQTTSNPacket)
      - AAD and the input to SHA-256 for nonce/IV derivation (AEAD schemes)

    The byte sequence is:
      length field (1 or 3 bytes) +
      packet-type byte (1 byte, PROTECTION_ENCAPSULATION = 0xFF) +
      ProtectionFlags (1 byte) +
      ProtectionScheme (1 byte) +
      SenderIdentifier (8 bytes) +
      Random (4 bytes) +
      CryptographicMaterial (0/2/4/12 bytes) +
      MonotonicCounter (0/2/4 bytes)

    We reconstruct this from the parsed packet fields rather than keeping
    the raw buffer, which avoids threading raw bytes through the call chain.
    """
    from mqtt.formats.MQTTSN2 import PacketTypes

    pf = packet.ProtectionFlags

    # Reconstruct the fixed portion of the body that precedes the inner packet
    body_prefix = (
        bytes([PacketTypes.PROTECTION_ENCAPSULATION]) +
        pf.pack() +
        bytes([packet.ProtectionScheme]) +
        packet.SenderIdentifier +          # always 8 bytes
        packet.Random +                    # always 4 bytes
        packet.CryptographicMaterial +     # 0, 2, 4 or 12 bytes
        packet.MonotonicCounter            # 0, 2 or 4 bytes
    )

    # Prepend the variable-length packet-length header
    total_len = 1 + len(body_prefix) + len(packet.ProtectedMQTTSNPacket) + len(packet.AuthenticationTag)
    if total_len <= 255:
        length_field = bytes([total_len])
    else:
        full_len = total_len + 2   # 3-byte length header
        length_field = bytes([0x01, (full_len >> 8) & 0xFF, full_len & 0xFF])

    return length_field + body_prefix


def _derive_nonce(aad_prefix: bytes, nonce_bytes: int) -> bytes:
    """Derive a nonce/IV by SHA-256 of the AAD prefix, truncated to nonce_bytes.

    [MQTT-SN-3.17.3-2/3/4]: truncate to leftmost bits.
    """
    digest = hashlib.sha256(aad_prefix).digest()
    return digest[:nonce_bytes]


# ---------------------------------------------------------------------------
# Tag-length resolution
# ---------------------------------------------------------------------------

def _resolve_tag_length(pf_auth_tag_len: int, nominal_tag_bytes: int, auth_only: bool) -> int:
    """Return the expected authentication tag length in bytes.

    pf_auth_tag_len   : ProtectionFlags.AuthTagLen (0–15, 2/3 rejected at unpack)
    nominal_tag_bytes : nominal tag size for the chosen scheme
    auth_only         : True if the scheme is authentication-only

    Spec rules applied:
      0x0 → provider-defined; we use the nominal size (same as 0x1)
      0x1 → nominal tag size  [MQTT-SN-3.17.2.3-2]
      0x4–0xF → AuthTagLen × 2 bytes, auth-only only [MQTT-SN-3.17.2.3-5/6]
    """
    if pf_auth_tag_len in (0x0, 0x1):
        return nominal_tag_bytes
    # 0x4–0xF truncation (only for auth-only; non-auth-only must use 0x1 per 3.17.2.3-1)
    if 0x4 <= pf_auth_tag_len <= 0xF:
        if not auth_only:
            raise ProtectionError(
                "AuthTagLen truncation values 0x4-0xF are only valid for "
                "Authentication Only schemes",
                "[MQTT-SN-3.17.2.3-4]"
            )
        tag_bytes = pf_auth_tag_len * 2   # [MQTT-SN-3.17.2.3-6]: tag = AuthTagLen × 16 bits
        if tag_bytes > nominal_tag_bytes:
            raise ProtectionError(
                f"AuthTagLen 0x{pf_auth_tag_len:X} requests {tag_bytes*8} bits, "
                f"exceeding nominal tag size {nominal_tag_bytes*8} bits",
                "[MQTT-SN-3.17.2.3-8]"
            )
        return tag_bytes
    # 0x2 and 0x3 are rejected by ProtectionFlags.unpack() already
    raise ProtectionError(f"Invalid AuthTagLen value 0x{pf_auth_tag_len:X}")


# ---------------------------------------------------------------------------
# Authentication-only verification
# ---------------------------------------------------------------------------

def _hmac_sha256(key: bytes, message: bytes) -> bytes:
    return hmac.new(key, message, hashlib.sha256).digest()


def _hmac_sha3_256(key: bytes, message: bytes) -> bytes:
    return hmac.new(key, message, hashlib.sha3_256).digest()


def _cmac(key: bytes, message: bytes) -> bytes:
    """AES-CMAC per RFC 4493."""
    c = CMAC(algorithms.AES(key))
    c.update(message)
    return c.finalize()


# Maps scheme code → MAC function (key, message) -> full-length tag bytes
_AUTH_ONLY_FNS = {
    0x00: _hmac_sha256,
    0x01: _hmac_sha3_256,
    0x02: _cmac,
    0x03: _cmac,
    0x04: _cmac,
}


def _verify_auth_only(scheme_code: int, key: bytes, aad_prefix: bytes,
                      inner_packet_bytes: bytes, expected_tag: bytes,
                      tag_bytes: int) -> bytes:
    """Verify an authentication-only scheme tag.

    The MAC input is the full AAD prefix (header fields) concatenated with
    the ProtectedMQTTSNPacket bytes [MQTT-SN-3.17.9].

    Returns inner_packet_bytes on success; raises ProtectionError on failure.
    """
    mac_fn = _AUTH_ONLY_FNS[scheme_code]
    message = aad_prefix + inner_packet_bytes
    full_tag = mac_fn(key, message)
    computed_tag = full_tag[:tag_bytes]   # leftmost bits for truncation [MQTT-SN-3.17.2.3-7]

    if not hmac.compare_digest(computed_tag, expected_tag):
        raise ProtectionError("Authentication tag mismatch", "[MQTT-SN-3.17.9]")

    logger.debug("[MQTT-SN-3.17.9] Authentication tag verified (scheme 0x%02X)", scheme_code)
    return inner_packet_bytes


# ---------------------------------------------------------------------------
# AEAD verification and decryption
# ---------------------------------------------------------------------------

def _verify_aead(scheme_code: int, key: bytes, aad_prefix: bytes,
                 ciphertext_with_tag: bytes) -> bytes:
    """Verify and decrypt an AEAD-protected inner packet.

    For AEAD schemes the ProtectedMQTTSNPacket field carries ciphertext;
    the AuthenticationTag was appended to it by the sender and is verified
    internally by the AEAD primitive.

    The nonce/IV is derived from the AAD prefix via SHA-256 [§3.17.3].

    Returns plaintext bytes on success; raises ProtectionError on failure.
    """
    is_ccm = 0x40 <= scheme_code <= 0x45
    is_gcm = 0x46 <= scheme_code <= 0x48
    is_chacha = scheme_code == 0x49

    if is_ccm:
        # CCM nonce: 13 bytes [MQTT-SN-3.17.3-2]
        nonce = _derive_nonce(aad_prefix, 13)
        _, _, key_bytes, tag_bytes = _SCHEMES[scheme_code]
        aead = AESCCM(key, tag_length=tag_bytes)
        try:
            plaintext = aead.decrypt(nonce, ciphertext_with_tag, aad_prefix)
        except InvalidTag:
            raise ProtectionError("AEAD authentication tag invalid (CCM)", "[MQTT-SN-3.17.9]")

    elif is_gcm:
        # GCM IV: 12 bytes [MQTT-SN-3.17.3-3]
        iv = _derive_nonce(aad_prefix, 12)
        aead = AESGCM(key)
        try:
            plaintext = aead.decrypt(iv, ciphertext_with_tag, aad_prefix)
        except InvalidTag:
            raise ProtectionError("AEAD authentication tag invalid (GCM)", "[MQTT-SN-3.17.9]")

    elif is_chacha:
        # ChaCha20/Poly1305 nonce: 12 bytes [MQTT-SN-3.17.3-4]
        nonce = _derive_nonce(aad_prefix, 12)
        aead = ChaCha20Poly1305(key)
        try:
            plaintext = aead.decrypt(nonce, ciphertext_with_tag, aad_prefix)
        except InvalidTag:
            raise ProtectionError(
                "AEAD authentication tag invalid (ChaCha20/Poly1305)", "[MQTT-SN-3.17.9]"
            )
    else:
        raise ProtectionError(f"Unrecognised AEAD scheme code 0x{scheme_code:02X}")

    logger.debug("[MQTT-SN-3.17.9] AEAD tag verified and inner packet decrypted (scheme 0x%02X)",
                 scheme_code)
    return plaintext


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def verify_protection(packet, key_store) -> bytes | None:
    """Verify the AuthenticationTag in a ProtectionEncapsulations packet.

    For AEAD schemes the ProtectedMQTTSNPacket field is decrypted as a
    side-effect of tag verification; its plaintext is returned.
    For authentication-only schemes the inner packet bytes are returned
    unchanged after the MAC is verified.

    Parameters
    ----------
    packet    : mqtt.formats.MQTTSN2.ProtectionEncapsulations
    key_store : object with lookup_key(sender_id: bytes) -> bytes | None

    Returns
    -------
    bytes  Plaintext bytes of the inner MQTT-SN packet.
    None   Verification failed; caller must send DISCONNECT and drop the client.
    """
    sender_id = packet.SenderIdentifier
    scheme_code = packet.ProtectionScheme
    pf = packet.ProtectionFlags

    # ---- Look up the shared key ------------------------------------------
    key = key_store.lookup_key(sender_id)
    if key is None:
        logger.warning("[MQTT-SN-3.17.4-1] No key registered for SenderIdentifier %s",
                       sender_id.hex())
        return None

    # ---- Validate scheme code --------------------------------------------
    if scheme_code not in _SCHEMES:
        logger.error("[MQTT-SN-3.17.3-1] Unknown or reserved ProtectionScheme 0x%02X", scheme_code)
        return None

    scheme_name, auth_only, required_key_bytes, nominal_tag_bytes = _SCHEMES[scheme_code]

    # ---- Validate key length for fixed-key schemes -----------------------
    if required_key_bytes is not None and len(key) != required_key_bytes:
        logger.error(
            "[MQTT-SN-3.17.3-1] Scheme %s requires %d-byte key; got %d bytes",
            scheme_name, required_key_bytes, len(key)
        )
        return None

    # ---- Validate non-auth-only AuthTagLen constraint -------------------
    # [MQTT-SN-3.17.2.3-1]: for non-auth-only schemes, AuthTagLen MUST be 0x1
    if not auth_only and pf.AuthTagLen not in (0x0, 0x1):
        logger.error(
            "[MQTT-SN-3.17.2.3-1] Non-auth-only scheme %s requires AuthTagLen 0x1, got 0x%X",
            scheme_name, pf.AuthTagLen
        )
        return None

    try:
        tag_bytes = _resolve_tag_length(pf.AuthTagLen, nominal_tag_bytes, auth_only)
    except ProtectionError as exc:
        logger.error("Tag length error: %s", exc)
        return None

    # ---- Build AAD prefix (also used as HMAC/CMAC input prefix) ---------
    aad_prefix = _build_aad_prefix(packet)

    # ---- Dispatch to the appropriate verification function ---------------
    try:
        if auth_only:
            expected_tag = packet.AuthenticationTag
            if len(expected_tag) != tag_bytes:
                raise ProtectionError(
                    f"Expected {tag_bytes}-byte tag, got {len(expected_tag)} bytes",
                    "[MQTT-SN-3.17.9]"
                )
            plaintext = _verify_auth_only(
                scheme_code, key, aad_prefix,
                packet.ProtectedMQTTSNPacket, expected_tag, tag_bytes
            )
        else:
            # For AEAD schemes the ciphertext and tag are passed together;
            # the cryptography library handles their separation internally.
            ciphertext_with_tag = packet.ProtectedMQTTSNPacket + packet.AuthenticationTag
            plaintext = _verify_aead(scheme_code, key, aad_prefix, ciphertext_with_tag)

    except ProtectionError as exc:
        logger.warning("Protection verification failed for sender %s: %s",
                       sender_id.hex(), exc)
        return None

    return plaintext


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------


def wrap_protection(inner_packet_bytes: bytes, sender_id: bytes,
                    scheme_code: int, key_store) -> "ProtectionEncapsulations":
    """Wrap inner_packet_bytes in a Protection Encapsulation and compute the tag.

    Parameters
    ----------
    inner_packet_bytes : bytes
        The already-packed bytes of the inner MQTT-SN packet to protect.
    sender_id : bytes
        The 8-byte Sender Identifier of the party wrapping the packet
        (the server's own identifier when wrapping outbound packets).
    scheme_code : int
        Protection scheme index from Table 3-39 (e.g. 0x00 for HMAC-SHA256).
    key_store : KeyStore
        Used to look up the shared key by sender_id.

    Returns
    -------
    ProtectionEncapsulations
        A fully populated packet object ready for .pack().

    Raises
    ------
    ProtectionError
        If the sender_id has no registered key, or the scheme is unsupported.
    """
    import os
    from mqtt.formats.MQTTSN2 import ProtectionEncapsulations, ProtectionFlags

    if scheme_code not in _SCHEMES:
        raise ProtectionError(f"Unknown or reserved ProtectionScheme 0x{scheme_code:02X}",
                              "[MQTT-SN-3.17.3-1]")

    key = key_store.lookup_key(sender_id)
    if key is None:
        raise ProtectionError(
            f"No key registered for SenderIdentifier {sender_id.hex()}",
            "[MQTT-SN-3.17.4-1]"
        )

    scheme_name, auth_only, required_key_bytes, nominal_tag_bytes = _SCHEMES[scheme_code]

    if required_key_bytes is not None and len(key) != required_key_bytes:
        raise ProtectionError(
            f"Scheme {scheme_name} requires {required_key_bytes}-byte key; got {len(key)}",
            "[MQTT-SN-3.17.3-1]"
        )

    # Build the packet shell so we can derive the AAD prefix
    pkt = ProtectionEncapsulations()
    pkt.ProtectionScheme      = scheme_code
    pkt.SenderIdentifier      = sender_id
    pkt.Random                = os.urandom(4)
    pkt.CryptographicMaterial = b""
    pkt.MonotonicCounter      = b""
    # AuthTagLen 0x1 = nominal tag size [MQTT-SN-3.17.2.3-2]
    pkt.ProtectionFlags.AuthTagLen = 0x1

    if auth_only:
        # ProtectedMQTTSNPacket is plaintext; tag covers prefix + plaintext
        pkt.ProtectedMQTTSNPacket = inner_packet_bytes
        aad_prefix = _build_aad_prefix(pkt)
        mac_fn = _AUTH_ONLY_FNS[scheme_code]
        pkt.AuthenticationTag = mac_fn(key, aad_prefix + inner_packet_bytes)
    else:
        # AEAD: encrypt inner_packet_bytes; ciphertext+tag stored in packet
        is_ccm   = 0x40 <= scheme_code <= 0x45
        is_gcm   = 0x46 <= scheme_code <= 0x48
        is_chacha = scheme_code == 0x49

        # We need the AAD prefix without the inner packet, so set a placeholder
        pkt.ProtectedMQTTSNPacket = b""
        aad_prefix = _build_aad_prefix(pkt)

        if is_ccm:
            nonce = _derive_nonce(aad_prefix, 13)
            aead = AESCCM(key, tag_length=nominal_tag_bytes)
            ct_and_tag = aead.encrypt(nonce, inner_packet_bytes, aad_prefix)
        elif is_gcm:
            iv = _derive_nonce(aad_prefix, 12)
            aead = AESGCM(key)
            ct_and_tag = aead.encrypt(iv, inner_packet_bytes, aad_prefix)
        elif is_chacha:
            nonce = _derive_nonce(aad_prefix, 12)
            aead = ChaCha20Poly1305(key)
            ct_and_tag = aead.encrypt(nonce, inner_packet_bytes, aad_prefix)
        else:
            raise ProtectionError(f"Unrecognised AEAD scheme 0x{scheme_code:02X}")

        pkt.ProtectedMQTTSNPacket = ct_and_tag[:-nominal_tag_bytes]
        pkt.AuthenticationTag     = ct_and_tag[-nominal_tag_bytes:]

    return pkt


def unit_tests():
    import os

    # -- Helper: build a minimal ProtectionEncapsulations packet object ----
    class MockProtectionFlags:
        def __init__(self, auth_tag_len=1, crypto_len=0, counter_len=0):
            self.AuthTagLen = auth_tag_len
            self.CryptoLen  = crypto_len
            self.CounterLen = counter_len

        def pack(self):
            return bytes([(self.AuthTagLen << 4) |
                          ((self.CryptoLen & 0x03) << 2) |
                          (self.CounterLen & 0x03)])

    class MockPacket:
        def __init__(self, scheme, sender_id, inner_bytes,
                     auth_tag=b"", auth_tag_len=1,
                     crypto_material=b"", monotonic_counter=b""):
            from mqtt.formats.MQTTSN2 import PacketTypes
            self.messageType            = PacketTypes.PROTECTION_ENCAPSULATION
            self.ProtectionScheme       = scheme
            self.SenderIdentifier       = sender_id
            self.Random                 = os.urandom(4)
            self.CryptographicMaterial  = crypto_material
            self.MonotonicCounter       = monotonic_counter
            self.ProtectedMQTTSNPacket  = inner_bytes
            self.AuthenticationTag      = auth_tag
            self.ProtectionFlags        = MockProtectionFlags(auth_tag_len=auth_tag_len)

    ks = KeyStore()
    sender_id = b'\xAA\xBB\xCC\xDD\xEE\xFF\x00\x11'
    inner_bytes = bytes([0x04, 0x0C, 0x00, 0x01])  # minimal PINGREQ: len=4, type=12, PacketId=1

    # ---- HMAC-SHA256 (scheme 0x00) ----------------------------------------
    key_hmac = os.urandom(32)
    ks.add_key(sender_id, key_hmac)

    pkt = MockPacket(0x00, sender_id, inner_bytes)
    aad = _build_aad_prefix(pkt)
    full_tag = _hmac_sha256(key_hmac, aad + inner_bytes)
    pkt.AuthenticationTag = full_tag  # nominal 32 bytes

    result = verify_protection(pkt, ks)
    assert result == inner_bytes, f"HMAC-SHA256 verify failed: {result!r}"
    print("HMAC-SHA256: PASS")

    # ---- HMAC-SHA256 with truncated tag (AuthTagLen=0x8, 16 bytes) ---------
    pkt2 = MockPacket(0x00, sender_id, inner_bytes, auth_tag_len=0x8)
    aad2 = _build_aad_prefix(pkt2)
    full_tag2 = _hmac_sha256(key_hmac, aad2 + inner_bytes)
    pkt2.AuthenticationTag = full_tag2[:16]   # 0x8 × 2 = 16 bytes

    result2 = verify_protection(pkt2, ks)
    assert result2 == inner_bytes, f"HMAC-SHA256 truncated verify failed: {result2!r}"
    print("HMAC-SHA256 (truncated 128-bit tag): PASS")

    # ---- Wrong tag → None --------------------------------------------------
    pkt_bad = MockPacket(0x00, sender_id, inner_bytes)
    pkt_bad.AuthenticationTag = bytes(32)  # all-zero wrong tag
    result_bad = verify_protection(pkt_bad, ks)
    assert result_bad is None, "Expected None for bad tag"
    print("HMAC-SHA256 (wrong tag): PASS")

    # ---- Unknown sender → None --------------------------------------------
    pkt_unknown = MockPacket(0x00, b'\x00' * 8, inner_bytes)
    pkt_unknown.AuthenticationTag = bytes(32)
    result_unknown = verify_protection(pkt_unknown, ks)
    assert result_unknown is None, "Expected None for unknown sender"
    print("Unknown sender: PASS")

    # ---- HMAC-SHA3_256 (scheme 0x01) --------------------------------------
    ks.add_key(sender_id, key_hmac)   # reuse same key slot
    pkt3 = MockPacket(0x01, sender_id, inner_bytes)
    aad3 = _build_aad_prefix(pkt3)
    full_tag3 = _hmac_sha3_256(key_hmac, aad3 + inner_bytes)
    pkt3.AuthenticationTag = full_tag3
    result3 = verify_protection(pkt3, ks)
    assert result3 == inner_bytes, f"HMAC-SHA3_256 verify failed: {result3!r}"
    print("HMAC-SHA3_256: PASS")

    # ---- CMAC-128 (scheme 0x02) -------------------------------------------
    key_cmac = os.urandom(16)
    ks.add_key(sender_id, key_cmac)
    pkt4 = MockPacket(0x02, sender_id, inner_bytes)
    aad4 = _build_aad_prefix(pkt4)
    full_tag4 = _cmac(key_cmac, aad4 + inner_bytes)
    pkt4.AuthenticationTag = full_tag4
    result4 = verify_protection(pkt4, ks)
    assert result4 == inner_bytes, f"CMAC-128 verify failed: {result4!r}"
    print("CMAC-128: PASS")

    # ---- AES-GCM-128-128 (scheme 0x46) ------------------------------------
    key_gcm = os.urandom(16)
    ks.add_key(sender_id, key_gcm)
    pkt5 = MockPacket(0x46, sender_id, b"")   # inner_bytes will be ciphertext
    aad5 = _build_aad_prefix(pkt5)
    iv5  = _derive_nonce(aad5, 12)
    aead5 = AESGCM(key_gcm)
    ciphertext_and_tag = aead5.encrypt(iv5, inner_bytes, aad5)
    # ProtectedMQTTSNPacket = ciphertext, AuthenticationTag = tag (last 16 bytes)
    pkt5.ProtectedMQTTSNPacket = ciphertext_and_tag[:-16]
    pkt5.AuthenticationTag     = ciphertext_and_tag[-16:]
    result5 = verify_protection(pkt5, ks)
    assert result5 == inner_bytes, f"AES-GCM-128-128 verify failed: {result5!r}"
    print("AES-GCM-128-128: PASS")

    # ---- AES-CCM-128-128 (scheme 0x43) ------------------------------------
    key_ccm = os.urandom(16)
    ks.add_key(sender_id, key_ccm)
    pkt6 = MockPacket(0x43, sender_id, b"")
    aad6 = _build_aad_prefix(pkt6)
    nonce6 = _derive_nonce(aad6, 13)
    aead6 = AESCCM(key_ccm, tag_length=16)
    ct_and_tag6 = aead6.encrypt(nonce6, inner_bytes, aad6)
    pkt6.ProtectedMQTTSNPacket = ct_and_tag6[:-16]
    pkt6.AuthenticationTag     = ct_and_tag6[-16:]
    result6 = verify_protection(pkt6, ks)
    assert result6 == inner_bytes, f"AES-CCM-128-128 verify failed: {result6!r}"
    print("AES-CCM-128-128: PASS")

    # ---- ChaCha20/Poly1305 (scheme 0x49) ----------------------------------
    key_cc = os.urandom(32)
    ks.add_key(sender_id, key_cc)
    pkt7 = MockPacket(0x49, sender_id, b"")
    aad7 = _build_aad_prefix(pkt7)
    nonce7 = _derive_nonce(aad7, 12)
    aead7 = ChaCha20Poly1305(key_cc)
    ct_and_tag7 = aead7.encrypt(nonce7, inner_bytes, aad7)
    pkt7.ProtectedMQTTSNPacket = ct_and_tag7[:-16]
    pkt7.AuthenticationTag     = ct_and_tag7[-16:]
    result7 = verify_protection(pkt7, ks)
    assert result7 == inner_bytes, f"ChaCha20/Poly1305 verify failed: {result7!r}"
    print("ChaCha20/Poly1305: PASS")

    print("\nAll unit tests passed.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unit_tests()
