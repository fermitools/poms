# TODO

Known issues and deferred cleanups. Each entry records where the problem is and
why it was left, so it can be picked up later without the original context.

## Bugs

### `get_project_stats()` crashes on unexpected or missing handle state

`webservice/DMRService.py:512`

Two defects in the same short function:

1. `retval[handle.get("state")] += 1` indexes a dict pre-seeded with only
   `initial`/`done`/`reserved`/`failed`. Any other state raises `KeyError`, and a
   handle with no `state` key raises `KeyError: None`. The identical counting at
   `DMRService.py:722` is guarded with `if file_state in project_fids_dict:`, so
   the defensive pattern is already the house style; this is the one place that
   skipped it.

2. If `handles` is `None` and `project_id` is `None`, the guard does not fire and
   `for handle in None` raises `TypeError`. This path is reachable from
   `get_project_handles()` (`DMRService.py:446`), which passes
   `handles=project_info.get("file_handles", []) if project_info else None` with
   no `project_id`. When `dd_client.get_project()` returns nothing the call
   raises, breaking the project-handles expander in the Data Dispatcher UI.

Suggested fix, which keeps counts identical when states are as expected:

```python
for handle in (handles or []):
    state = handle.get("state")
    if state in retval:
        retval[state] += 1
```

Also worth deciding whether `get_project_handles()` should handle a falsy
`project_info` explicitly — the `project_details` comprehension on the same
return line would also raise on `None`.

## Deferred cleanup

### Unused SAM file-stat values in `campaign_stage_submissions`

`webservice/StagesPOMS.py:1091`

`get_file_stats_for_submissions()` returns a ten-value tuple; seven of them
(`some_kids_decl_needed`, `some_kids_needed`, `base_dim_list`,
`all_kids_decl_needed`, `some_kids_list`, `some_kids_decl_list`,
`all_kids_decl_list`) are unpacked and never used here, and `psummary`
(`StagesPOMS.py:1182`) is assigned and never read.

These are not free — they are real SAM round-trips paid for on every page load.
Before deleting, decide whether the page should be *using* them (they carry
child-file and consumption counts that the File Status Summary could show) or
whether the fetch should be skipped.

Note the same tuple is unpacked at `CampaignsPOMS.py:412` and
`SubmissionsPOMS.py:1094`, so changing the return signature touches all three
call sites. `SubmissionsPOMS.py:1096` has the same dead `psummary` assignment.

### File Status Summary duplicates the Available Output column

`webservice/templates/campaign_stage_submissions.html`

For SAM submissions the File Status Summary cell shows a single row,
`Available output: N`, which is the same number already displayed in the
Available Output column immediately to its left (`rec.available_output`,
`StagesPOMS.py:1227`). Either drop one, or give the summary cell the fuller
breakdown that its name implies — the SAM equivalent of what Data Dispatcher
rows now show.

### Duplicate `confirm_call` script block

`webservice/templates/campaign_stage_submissions.html:31` and `:45`

The same `confirm_call()` function is defined twice, character for character, in
two adjacent `<script>` blocks. The second definition simply overwrites the
first. Delete one.

### Data Dispatcher web UI URL is hardcoded

`webservice/DMRService.py` (`get_data_dispatcher_project_url`) and
`webservice/templates/submission_details.html:142`

Both build `https://metacat.fnal.gov:9443/{experiment}_dd[_prod]/gui/P/project`
from a hardcoded host and namespace. It belongs in the Shrek config, but the
existing `DATA_DISPATCHER_URL` (`DMRService.py:151`) points at the API server,
not the GUI, so a new config value is needed. Once it exists,
`submission_details.html:142` should use the shared helper rather than its own
copy of the string.

## Larger items, not yet triaged

Observed while reading the code; each needs a discussion about blast radius
before anyone touches it.

- `webservice/service.py:277` — the `if __name__ == "__main__":` guard is
  commented out in favour of `run_it = True`, so the whole service starts at
  import time. This is what makes `application = cherrypy.tree` work for WSGI,
  but it also means the module cannot be imported for any other purpose,
  including testing.
- `webservice/static/js/` vendors two versions of jQuery (2.1.4 and 2.2.4).
- `setup.cfg` declares `version = v4_4_2` and Python 3.8 while the repo is at
  v5.2.0; `pyproject.toml` targets py36/py37 for Black.
- `ddl/` migrations are hand-applied dated SQL files with no tooling to track
  what has been applied to which instance.
