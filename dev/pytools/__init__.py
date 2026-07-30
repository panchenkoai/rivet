"""Python replacements for rivet's dev shell scripts.

`shell.py` holds the shared plumbing (process running, preconditions, output);
each module beside it is one former `.sh`, kept behaviour-identical so a
transcript diff can verify the port instead of trust.

See `shell.py`'s docstring for the specific bash failure modes each helper
removes — every one of them cost this repo a real incident.
"""
