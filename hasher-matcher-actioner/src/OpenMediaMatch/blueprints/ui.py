# Copyright (c) Meta Platforms, Inc. and affiliates.

import time
import typing as t

from flask import Blueprint, abort, render_template, current_app
from flask import request, redirect

from OpenMediaMatch.blueprints import matching, curation, hashing
from OpenMediaMatch.persistence import get_storage
from OpenMediaMatch.utils.time_utils import duration_to_human_str

bp = Blueprint("ui", __name__)


def _index_info() -> dict[str, dict[str, t.Any]]:
    index = curation.signal_type_index_status()
    for name, dat in index.items():
        progress = 100
        progress_label = "Up to date"
        if dat["index_out_of_date"]:
            a = dat["index_size"]
            b = dat["db_size"]
            pct = min(a, b) * 100 / max(1, a, b)
            progress = int(min(90, pct))
            progress_label = f"{min(a, b)}/{max(a, b)} ({pct:.2f}%)"
        index[name]["progress_pct"] = progress
        index[name]["progress_label"] = progress_label
        index[name]["progress_style"] = "bg-success" if progress == 100 else ""
    return index


def _api_cls_info() -> dict[str, dict[str, t.Any]]:
    return {
        name: {
            "auth_icon": (
                ""
                if not cfg.supports_auth
                else ("🔒" if cfg.credentials is None else "🔑")
            ),
            "auth_title": (
                ""
                if not cfg.supports_auth
                else (
                    'title="May need credentials"'
                    if cfg.credentials is None
                    else 'title="Has credentials"'
                )
            ),
        }
        for name, cfg in get_storage().exchange_apis_get_configs().items()
    }


def _collab_info() -> dict[str, dict[str, t.Any]]:
    storage = get_storage()
    collabs = storage.exchanges_get()
    ret = {}
    for name, cfg in collabs.items():
        # serial db fetch, yay!
        fetch_status = storage.exchange_get_fetch_status(name)
        progress_label = ""
        progress = 50
        if fetch_status.last_fetch_complete_ts is None:
            progress = 0
        elif fetch_status.up_to_date:
            progress_label = "Up to date!"
            progress = 100
        # TODO add some idea of progress to the checkpoint class

        progress_style = "bg-success" if progress == 100 else "bg-info"
        if fetch_status.last_fetch_succeeded is False:
            progress_style = "bg-danger"
            progress_label = "Error on fetch!"
            progress = min(90, progress)

        last_run_time = fetch_status.last_fetch_complete_ts
        if fetch_status.running_fetch_start_ts is not None:
            progress_style += " progress-bar-striped progress-bar-animated"
            last_run_time = fetch_status.running_fetch_start_ts

        if not cfg.enabled:
            progress_style = "bg-secondary"

        last_run_text = "Never"
        if last_run_time is not None:
            diff = max(int(time.time() - last_run_time), 0)
            last_run_text = duration_to_human_str(diff, terse=True)
            last_run_text += " ago"

        ret[name] = {
            "api": cfg.api,
            "bank": name.removeprefix("c-"),
            "enabled": cfg.enabled,
            "count": fetch_status.fetched_items,
            "progress_style": progress_style,
            "progress_pct": progress,
            "progress_label": progress_label,
            "last_run_text": last_run_text,
        }
    return ret


@bp.route("/")
def home():
    """
    UI Landing page
    """

    # Check if SEED_BANK_0 and SEED_BANK_1 exist yet
    bank_list = curation.banks_index()
    contains_seed_bank_0 = any(bank.name == "SEED_BANK_0" for bank in bank_list)
    contains_seed_bank_1 = any(bank.name == "SEED_BANK_1" for bank in bank_list)

    template_vars = {
        "signal": curation.get_all_signal_types(),
        "content": curation.get_all_content_types(),
        "exchange_apis": _api_cls_info(),
        "production": current_app.config.get("PRODUCTION", False),
        "index": _index_info(),
        "collabs": _collab_info(),
        "is_banks_seeded": contains_seed_bank_0 and contains_seed_bank_1,
    }
    return render_template("bootstrap.html.j2", page="home", **template_vars)


@bp.route("/banks")
def banks():
    """
    Bank management page
    """
    template_vars = {
        "bankList": curation.banks_index(),
        "content": curation.get_all_content_types(),
        "signal": curation.get_all_signal_types(),  # Add signal types for hash input dropdown
    }
    return render_template("bootstrap.html.j2", page="banks", **template_vars)


@bp.route("/exchanges")
def exchanges():
    """
    Exchange management page
    """
    template_vars = {
        "exchange_apis": _api_cls_info(),
        "collabs": _collab_info(),
    }
    return render_template("bootstrap.html.j2", page="exchanges", **template_vars)


@bp.route("/match")
def match_dbg():
    """
    Bank management page
    """
    return render_template("bootstrap.html.j2", page="match_dbg")


@bp.route("/create_bank", methods=["POST"])
def ui_create_bank():
    # content type from dropdown form
    bank_name = request.form.get("bank_name")
    if bank_name is None:
        abort(400, "Bank name is required")
    curation.bank_create_impl(bank_name)
    return redirect("./")


@bp.route("/query", methods=["POST"])
def upload():
    current_app.logger.debug("[query] hashing input")
    signals = hashing.hash_media_from_form_data()

    current_app.logger.debug("[query] performing lookup")
    bank_matches = {}
    for st_name, signal in signals.items():
        matches = matching.lookup(signal, st_name)
        for bank_name, bank_results in matches.items():
            min_distance = min(float(match["distance"]) for match in bank_results)
            if bank_name not in bank_matches or min_distance < bank_matches[bank_name]["distance"]:
                bank_matches[bank_name] = {"distance": min_distance}

    return {
        "hashes": signals, 
        "banks": sorted(bank_matches.keys()),
        "bank_matches": bank_matches
    }


@bp.route("/query_hash", methods=["POST"])
def query_hash():
    """
    Look up a hash directly in the similarity index.
    
    Input:
     * hash - the hash value
     * signal_type - the signal type name
     
    Output:
     * JSON object with banks that match, the hash value, and distance information
    """
    current_app.logger.debug("[query_hash] processing direct hash input")
    
    hash_value = request.form.get("hash")
    signal_type = request.form.get("signal_type")
    
    if not hash_value or not signal_type:
        abort(400, "Both hash and signal_type are required")
    
    current_app.logger.debug("[query_hash] performing lookup for %s hash: %s", signal_type, hash_value)
    matches = matching.lookup(hash_value, signal_type)
    
    bank_matches = {}
    for bank_name, bank_results in matches.items():
        min_distance = min(float(match["distance"]) for match in bank_results)
        bank_matches[bank_name] = {"distance": min_distance}
    
    # Return the same format as the file upload endpoint
    return {
        "hashes": {signal_type: hash_value}, 
        "banks": sorted(matches.keys()),
        "bank_matches": bank_matches
    }


@bp.route("/add_hash_to_bank", methods=["POST"])
def add_hash_to_bank():
    """
    Add a hash directly to a bank.
    
    Input:
     * hash - the hash value
     * signal_type - the signal type name
     * bank_name - the bank to add the hash to
     
    Output:
     * JSON object with the content ID and signals added
    """
    current_app.logger.debug("[add_hash_to_bank] processing direct hash addition to bank")
    
    hash_value = request.form.get("hash")
    signal_type = request.form.get("signal_type")
    bank_name = request.form.get("bank_name")
    
    if not hash_value or not signal_type or not bank_name:
        abort(400, "hash, signal_type, and bank_name are all required")
    
    # Validate the bank exists
    storage = get_storage()
    bank = storage.get_bank(bank_name)
    if not bank:
        abort(404, f"bank '{bank_name}' not found")
    
    # At this point bank is guaranteed to be not None
    
    # Validate the signal type exists and is enabled
    signal_type_cfgs = storage.get_signal_type_configs()
    st_cfg = signal_type_cfgs.get(signal_type)
    if st_cfg is None:
        abort(400, f"No such signal type {signal_type}")
    if st_cfg.enabled_ratio <= 0:
        abort(400, f"Signal type {signal_type} is disabled")
    
    # Validate the hash format
    try:
        validated_hash = st_cfg.signal_type.validate_signal_str(hash_value)
    except Exception as e:
        abort(400, f"Invalid {signal_type} hash: {str(e)}")
    
    current_app.logger.debug("[add_hash_to_bank] adding %s hash to bank %s: %s", signal_type, bank_name, hash_value)
    
    # Use the existing bank_add_content method to add the hash
    from OpenMediaMatch.storage import interface as iface
    
    signals = {st_cfg.signal_type: validated_hash}
    content_config = iface.BankContentConfig(
        id=0,
        disable_until_ts=iface.BankContentConfig.ENABLED,
        collab_metadata={},
        original_media_uri=None,
        bank=bank,  # type: ignore  # bank is guaranteed to be not None due to check above
    )
    
    content_id = storage.bank_add_content(bank_name, signals, content_config)
    
    # Return the same format as the file upload endpoint
    return {
        "id": content_id,
        "signals": {st_cfg.signal_type.get_name(): validated_hash},
    }
