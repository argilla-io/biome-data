# See https://www.python.org/dev/peps/pep-0440/ for version numbering
__version__ = "0.2.0.dev"


def get_commit_hash() -> str:
    """
    Fetch current commit hash from configured git repository. The working folder should
    be part of a already git repository

    """
    try:
        import git

        repo = git.Git()
        return repo.log("--pretty=format:'%h'", "-n 1").replace("'", "")
    except Exception as e:
        print(e)
        return ""


if ".dev" in __version__:
    __version__ += f"+{get_commit_hash()}"
