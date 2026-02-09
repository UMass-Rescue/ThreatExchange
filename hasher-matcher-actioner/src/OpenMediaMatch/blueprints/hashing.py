# Copyright (c) Meta Platforms, Inc. and affiliates.

"""
Endpoints for hashing content
"""

from pathlib import Path
import tempfile
import typing as t
import requests
import logging
from urllib.parse import urlparse
import ipaddress
import socket
import os
from contextlib import contextmanager

from flask import Blueprint
from flask import abort, request, current_app
from werkzeug.exceptions import HTTPException

from threatexchange.content_type.content_base import ContentType
from threatexchange.content_type.photo import PhotoContent
from threatexchange.content_type.video import VideoContent
from threatexchange.signal_type.signal_base import FileHasher, BytesHasher, SignalType

from OpenMediaMatch.persistence import get_storage
from OpenMediaMatch.utils import flask_utils
from OpenMediaMatch.storage.interface import BankConfig

logger = logging.getLogger(__name__)

bp = Blueprint("hashing", __name__)
bp.register_error_handler(HTTPException, flask_utils.api_error_handler)

# Add these constants at the top level
DEFAULT_MAX_REMOTE_FILE_SIZE = 100 * 1024 * 1024  # 100MB max file size


def is_valid_url(url: str) -> bool:
    """
    Validate URL to prevent SSRF attacks.
    Returns True if URL is safe, False otherwise.
    """
    try:
        parsed = urlparse(url)

        if not parsed.scheme or not parsed.netloc:
            return False

        if parsed.scheme not in ("http", "https"):
            return False

        hostname = parsed.netloc.split(":")[0]
        # Check for malformed URLs with invalid port numbers
        if ":" in parsed.netloc and not parsed.netloc.split(":")[1].isdigit():
            return False

        # For testing, allow GitHub domains
        if hostname in ("github.com", "raw.githubusercontent.com"):
            return True

        # Check if there is an allowlist and hostname matches.
        allowed_hostnames = current_app.config.get("ALLOWED_HOSTNAMES", set())
        if not allowed_hostnames:
            return True

        if any(
            hostname == allowed or hostname.endswith(f".{allowed}")
            for allowed in allowed_hostnames
        ):
            return True

        return False
    except Exception as e:
        logger.warning(f"URL validation error: {str(e)}")
        return False


def _check_content_length_stream_response(
    url: str, max_length: int = DEFAULT_MAX_REMOTE_FILE_SIZE
) -> requests.Response:
    """
    Check for content length and raise an exception if it exceeds max_length.
    Returns the response as a stream.
    """
    response = requests.get(url, stream=True, timeout=30, allow_redirects=True)
    response.raise_for_status()

    content_length = response.headers.get("content-length")
    if content_length is not None and int(content_length) > max_length:
        abort(413, "Content too large")

    return response


@bp.route("/hash", methods=["GET"])
def hash_media() -> dict[str, str]:
    """
    Fetch content and return its hash.

    Input:
        * url - path to the media to hash. Supports image or video.

    Output:
        * Mapping of signal types to hash values. Signal types are derived from the content type of the provided URL
    """
    media_url = request.args.get("url", None)
    if media_url is None:
        abort(400, "Missing required parameter: url")

    try:
        return hash_url_content(media_url)
    except ValueError as e:
        abort(400, str(e))


def hash_url_content(media_url: str) -> dict[str, str]:
    """
    Utility function to hash content from a URL.

    Args:
        media_url: URL to the media content

    Returns:
        Mapping of signal types to hash values

    Raises:
        ValueError: If URL is invalid or content cannot be hashed
    """
    if not is_valid_url(media_url):
        abort(400, "Invalid or unsafe URL provided")

    try:
        # Get response with content length tracking
        max_file_size = (
            current_app.config.get("MAX_REMOTE_FILE_SIZE")
            or DEFAULT_MAX_REMOTE_FILE_SIZE
        )

        # cast to integer if necessary (the value could have come from an environment variable)
        if isinstance(max_file_size, str):
            if not max_file_size.isdigit():
                logger.error(
                    f"MAX_REMOTE_FILE_SIZE misconfigured, expected integer, received: {max_file_size}"
                )
                abort(500, "Service misconfigured, see logs for details")

            max_file_size = int(max_file_size)

        with _check_content_length_stream_response(
            media_url, max_file_size
        ) as download_resp:
            url_content_type = download_resp.headers["content-type"]
            current_app.logger.debug("%s is type %s", media_url, url_content_type)

            content_type = _parse_request_content_type(url_content_type)
            signal_types = _parse_request_signal_type(content_type)

            ret: dict[str, str] = {}

            # For images, we may need to copy the file suffix (.png, jpeg, etc) for it to work
            with tempfile.NamedTemporaryFile("wb") as tmp:
                current_app.logger.debug("Writing to %s", tmp.name)
                bytes_read = 0
                with tmp.file as temp_file:  # this ensures that bytes are flushed before hashing
                    for chunk in download_resp.iter_content(chunk_size=8192):
                        if chunk:
                            bytes_read += len(chunk)
                            # Check as we write the file to ensure we don't exceed the max content length
                            if bytes_read > max_file_size:
                                abort(413, "Content too large")
                            temp_file.write(chunk)
                path = Path(tmp.name)
                for st in signal_types.values():
                    if issubclass(st, FileHasher):
                        ret[st.get_name()] = st.hash_from_file(path)
            return ret
    except requests.exceptions.RequestException as e:
        abort(400, f"Failed to fetch URL: {str(e)}")


@bp.route("/hash", methods=["POST"])
def hash_media_post():
    """
    Calculate the hash for the provided file.
    """

    return hash_media_from_form_data()


def hash_media_from_form_data() -> dict[str, str]:
    """
    Hash the provided file and return the hash values.

    Input:
        * files - the multipart/form-data to hash (only one file allowed)

    Output:
        * Mapping of signal types to hash values
    """
    if not request.files:
        return abort(400, "Missing multipart/form-data file upload")

    # Let's just accept a single file per request. This keeps it consistent with the GET method.
    if len(request.files) > 1:
        abort(400, "Only one file allowed per request")

    ret = {}

    # Each file in a multipart/form-data body has a name as well as a filename:
    # Content-Disposition: form-data; name="field1"; filename="example.txt"
    # We will require that the field name is one of the supported content types.
    for field_name in request.files.keys():
        # The file key must be one of the supported content types.
        content_type = _lookup_content_type(field_name)
        signal_types = _parse_request_signal_type(content_type)

        if len(request.files.getlist(field_name)) > 1:
            abort(400, "Only one file allowed per request")

        for file in request.files.getlist(field_name):
            current_app.logger.debug(
                "Processing upload of type %s, filename=%s, mimetype=%s",
                field_name,
                file.filename,
                file.mimetype,
            )
            bytes = file.stream.read()
            for st in signal_types.values():
                if issubclass(st, BytesHasher):
                    ret[st.get_name()] = st.hash_from_bytes(bytes)

                elif issubclass(st, FileHasher):
                    with tempfile.NamedTemporaryFile("wb") as tmp:
                        current_app.logger.debug(
                            "Writing to %s for hashing, signal_type=%s",
                            tmp.name,
                            st.get_name(),
                        )
                        with tmp.file as temp_file:  # this ensures that bytes are flushed before hashing
                            temp_file.write(bytes)
                            path = Path(tmp.name)
                            ret[st.get_name()] = st.hash_from_file(path)

    return ret


@bp.route("/hash/batch", methods=["POST"])
def hash_media_batch():
    """
    Calculate hashes for multiple files in a single request.
    
    Optimized for batch processing, particularly beneficial for GPU-accelerated
    hashers where processing multiple images together provides
    5-10x throughput improvements.
    
    Input:
        * files - multipart/form-data with multiple files
        * Query params: signal_type (optional) - single signal type name. If omitted, all enabled signal types are computed.
    
    Output:
        * List of objects mapping signal types to hash values
    """
    if not request.files:
        abort(400, "Missing multipart/form-data file upload")
    
    # Collect all files
    files_data = []
    for field_name in request.files.keys():
        content_type = _lookup_content_type(field_name)
        all_signal_types = get_storage().get_enabled_signal_types_for_content_type(content_type)
        
        if not all_signal_types:
            abort(500, "No signal types configured!")
        
        # Filter to single signal type if specified
        signal_type_name = request.args.get("signal_type", None)
        if signal_type_name:
            signal_type_name = signal_type_name.strip()
            if signal_type_name not in all_signal_types:
                abort(400, f"Signal type '{signal_type_name}' doesn't exist or is disabled")
            signal_types = {signal_type_name: all_signal_types[signal_type_name]}
        else:
            signal_types = all_signal_types
        
        for file in request.files.getlist(field_name):
            files_data.append((file.filename or "unknown", file.stream.read(), signal_types))
    
    if not files_data:
        abort(400, "No files provided")
    
    # Check for batch-capable signal types (e.g., CLIP with hash_from_file_list)
    batch_capable = {}
    for st_name, st in files_data[0][2].items():
        if hasattr(st, 'hash_from_file_list') and issubclass(st, FileHasher):
            batch_capable[st_name] = st
    
    # Process batch-capable types together
    batch_results = {}
    if batch_capable:
        temp_files = []
        try:
            # Write all files to temp locations
            for filename, file_bytes, _ in files_data:
                tmp = tempfile.NamedTemporaryFile("wb", delete=False, suffix=Path(filename).suffix)
                tmp.write(file_bytes)
                tmp.close()
                temp_files.append(Path(tmp.name))
            
            # Batch process each batch-capable signal type
            for st_name, st in batch_capable.items():
                try:
                    batch_outputs = st.hash_from_file_list(temp_files)
                    # Convert CLIPOutput or other objects to strings
                    batch_results[st_name] = [str(h) for h in batch_outputs]
                except Exception as e:
                    current_app.logger.error("Batch hashing failed for %s: %s", st_name, str(e))
                    batch_results[st_name] = None
        finally:
            # Clean up temp files
            for temp_file in temp_files:
                try:
                    temp_file.unlink()
                except Exception:
                    pass
    
    # Build results for each file (results are returned in the same order as files were uploaded)
    results = []
    for idx, (filename, file_bytes, signal_types) in enumerate(files_data):
        file_result = {}
        
        for st_name, st in signal_types.items():
            try:
                if st_name in batch_results and batch_results[st_name]:
                    # Use batch result
                    file_result[st_name] = batch_results[st_name][idx]
                elif issubclass(st, BytesHasher):
                    file_result[st_name] = st.hash_from_bytes(file_bytes)
                elif issubclass(st, FileHasher):
                    with tempfile.NamedTemporaryFile("wb") as tmp:
                        tmp.file.write(file_bytes)
                        path = Path(tmp.name)
                        file_result[st_name] = st.hash_from_file(path)
            except Exception as e:
                current_app.logger.warning("Failed to hash %s with %s: %s", filename, st_name, str(e))
                file_result[st_name] = ""
        
        results.append(file_result)
    
    return results


def _parse_request_content_type(url_content_type: str) -> t.Type[ContentType]:
    arg = request.args.get("content_type", "")
    if not arg:
        if url_content_type.lower().startswith("image"):
            arg = PhotoContent.get_name()
        elif url_content_type.lower().startswith("video"):
            arg = VideoContent.get_name()
        else:
            abort(
                400,
                f"unsupported url ContentType: '{url_content_type}', "
                "if you know the expected type, provide it with the content_type query param",
            )
    return _lookup_content_type(arg)


def _lookup_content_type(arg: str) -> t.Type[ContentType]:
    content_type_config = get_storage().get_content_type_configs().get(arg)
    if content_type_config is None:
        abort(400, f"no such content_type: '{arg}'")

    if not content_type_config.enabled:
        abort(400, f"content_type {arg} is disabled")

    return content_type_config.content_type


def _parse_request_signal_type(
    content_type: t.Type[ContentType],
) -> t.Mapping[str, t.Type[SignalType]]:
    """
    Parse the signal types from the request args.
    """
    signal_types = get_storage().get_enabled_signal_types_for_content_type(content_type)
    if not signal_types:
        abort(500, "No signal types configured!")
    signal_type_args = request.args.get("types", None)
    if signal_type_args is None:
        return signal_types

    ret = {}
    for st_name in signal_type_args.split(","):
        st_name = st_name.strip()
        if not st_name:
            continue
        if st_name not in signal_types:
            abort(400, f"signal type '{st_name}' doesn't exist or is disabled")
        ret[st_name] = signal_types[st_name]

    if not ret:
        abort(400, "empty signal type selection")

    return ret
