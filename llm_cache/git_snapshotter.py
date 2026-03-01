import getpass
import logging
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Iterable, Tuple
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

SNAPSHOT_REF_PREFIX = "refs/snapshots"
SNAPSHOT_REF_GLOB = f"{SNAPSHOT_REF_PREFIX}/*"


class GitSnapshotter:
    def __init__(
        self,
        working_dir: Path,
        cache_repo: str | None = None,
        extra_gitignore: Iterable[str] | None = None,
    ):
        self.working_dir = working_dir.resolve()
        self.working_dir.mkdir(parents=True, exist_ok=True)
        self.current_hash: str | None = None

        # If there's already a repo here, verify it's rooted HERE (not a parent).
        # If not, initialize a new repo here.
        if self._has_git_dir_here():
            self._pin_repo_env()
            self._assert_repo_root_is_working_dir()
        else:
            # Create an independent repo here.
            # Ensure we don't accidentally "discover" a parent repo when calling git.
            self._git_raw(["init"])
            self._pin_repo_env()
            self._assert_repo_root_is_working_dir()

        # Minimal identity for commits
        username = getpass.getuser()
        self._git(["config", "user.name", username])
        self._git(["config", "user.email", "llm@local"])

        if extra_gitignore:
            self._write_extra_gitignore(extra_gitignore)

        self.cache_repo: str | None = None
        if cache_repo is not None:
            self.cache_repo = self._configure_root_remote(cache_repo, "cache_repo")
            self.fetch_snapshots()

    def snapshot(self, name: str) -> Tuple[str | None, str | None]:
        """
        Creates a snapshot commit.
        Returns (parent_hash, new_hash).
        """
        safe = self._sanitize_ref_component(f"snapshot-{name}")
        assert self.is_snapshot_name_unique(safe), (
            f'Snapshot with name "{name}" already exists'
        )
        parent = self._head_hash(allow_none=True)

        if parent is not None:
            self._git(["switch", "--detach", parent])

        self._git(["add", "-A"])
        self._git(
            [
                "commit",
                "--allow-empty",
                "-m",
                f"Snapshot {name.strip()}",
            ]
        )

        new = self._head_hash()
        self._git(["update-ref", self._snapshot_ref(safe), new])

        self.current_hash = new

        logger.debug(f"Created snapshot '{name}': {parent} -> {new}")

        return parent, new

    def restore(self, commit_hash: str) -> None:
        """
        Restores the working directory to the given commit.
        """
        self._git(["switch", "--detach", commit_hash])
        self._git(["reset", "--hard", commit_hash])

        self.current_hash = commit_hash

    def is_dirty(self) -> bool:
        """
        Returns True if there are uncommitted changes in the working directory.
        Includes staged, unstaged, and untracked files (except ignored ones).
        """
        result = self._git_capture(["status", "--porcelain"], check=False)
        return bool(result.stdout.strip())

    def clear_untracked(self, include_ignored: bool = False) -> None:
        """
        Delete files/dirs that are not tracked by git in this repo.

        - include_ignored=False: removes untracked files/dirs, keeps ignored files.
        - include_ignored=True: also removes ignored files (like build artifacts, caches).
        """
        args = ["clean", "-fd"]
        if include_ignored:
            args.append("-x")  # remove ignored files too
        self._git(args)

    def reset_changes(self) -> None:
        """
        Discard all local modifications to tracked files.
        Does NOT remove untracked or ignored files.
        Equivalent to: git reset --hard HEAD
        """
        self._git(["reset", "--hard", "HEAD"])

    def matches_snapshot(self, commit_hash: str) -> bool:
        """
        Returns True iff the current working directory exactly matches
        the given snapshot commit:
        - files tracked in commit_hash match filesystem contents
        - no extra untracked (non-ignored) files exist
        """

        # 1) Get files tracked in the snapshot commit
        tree = self._git_capture(["ls-tree", "-r", "--name-only", commit_hash])
        tracked_files = [p for p in tree.stdout.splitlines() if p]

        try:
            # 2) Intent-to-add files that exist on disk but are untracked in HEAD
            for path in tracked_files:
                self._git_quiet(["add", "-N", "--", path], check=False)

            # 3) Compare snapshot commit against working tree
            diff = self._git_quiet(["diff", "--quiet", commit_hash, "--", "."], check=False)
            if diff.returncode != 0:
                return False

            # 4) Ensure no extra untracked (non-ignored) files exist
            untracked = self._git_capture(["ls-files", "--others", "--exclude-standard"])
            return not bool(untracked.stdout.strip())

        finally:
            # 5) Clean index side-effects (remove intent-to-add entries)
            self._git_quiet(["reset", "--quiet"], check=False)

    def has_snapshot(self, commit_hash: str) -> bool:
        """
        Returns True iff the given commit hash exists in this repository.
        This checks object existence, not whether a ref points to it.
        """
        result = self._git_quiet(["cat-file", "-e", f"{commit_hash}^{{commit}}"], check=False)
        return result.returncode == 0

    def is_snapshot_name_unique(self, name: str) -> bool:
        """
        Returns True iff refs/snapshots/<name> does not already exist.
        """
        ref = f"refs/snapshots/{name}"
        ref = self._snapshot_ref(name)

        result = self._git_quiet(["show-ref", "--verify", "--quiet", ref], check=False)
        return result.returncode != 0

    def create_empty_snapshot(self, name: str) -> str:
        """
        Creates an empty commit (no files at all), anchors it at refs/snapshots/<name>,
        records a reflog message, and CHECKS IT OUT so the repo ends in empty state.

        This will:
        - remove untracked files (via clear_untracked)
        - reset tracked files away (via reset --hard) when checking out the empty commit
        - move HEAD (so it appears in `git log --reflog --oneline`)
        """
        safe = self._sanitize_ref_component(f"empty-{name}")
        assert self.is_snapshot_name_unique(safe), (
            "Empty Snapshot with this name already exists"
        )

        # Remove untracked stuff first (keeps ignored by default)
        self.clear_untracked(include_ignored=True)

        # Ensure reflogs are recorded for ref updates
        self._git(["config", "core.logAllRefUpdates", "true"])

        EMPTY_TREE = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"

        # Create commit directly from the empty tree
        result = self._git_capture(
            ["commit-tree", EMPTY_TREE, "-m", f"Empty Snapshot {name}".strip()]
        )
        commit_hash = result.stdout.strip()

        # Anchor snapshot ref + reflog message for that ref
        self._git(
            [
                "update-ref",
                "-m",
                f"create empty snapshot {name}",
                self._snapshot_ref(safe),
                commit_hash,
            ]
        )

        # CHECK IT OUT (detach HEAD) and enforce empty working tree state
        self._git(["switch", "--detach", commit_hash])
        self._git(["reset", "--hard", commit_hash])

        # Record current hash
        self.current_hash = commit_hash

        return commit_hash

    def print_tree(self) -> None:
        size = shutil.get_terminal_size(fallback=(120, 40))
        use_compact = size.columns < 120 or size.lines < 20
        args = ["log", "--oneline", "--decorate", "--all", "--date-order"]
        if not use_compact:
            args.insert(1, "--graph")
        if use_compact:
            args.insert(0, "--no-pager")
        self._git_run(args, check=True, passthrough=True)

    def push_snapshots(self) -> None:
        """
        Push all snapshot refs to the root repo (same namespace).
        If `root` is None, uses self.root (set via __init__).
        """
        if self.cache_repo is None:
            return

        self._git_run(["push", self.cache_repo, f"{SNAPSHOT_REF_GLOB}:{SNAPSHOT_REF_GLOB}"])

    def fetch_snapshots(self) -> None:
        """
        Fetch all snapshot refs from the root repo (same namespace).
        If `root` is None, uses self.root (set via __init__).
        """
        if self.cache_repo is None:
            return

        self._git_run(["fetch", self.cache_repo, f"{SNAPSHOT_REF_GLOB}:{SNAPSHOT_REF_GLOB}"])

    # ---------- git + safety helpers ----------

    def _configure_root_remote(self, root_repo: str, remote_name: str) -> str:
        resolved = None

        path = Path(root_repo).expanduser()
        if path.exists():
            resolved = path.resolve().as_uri()
        else:
            parsed = urlparse(root_repo)
            if parsed.scheme:
                resolved = root_repo
            elif self._remote_exists(root_repo):
                return root_repo
            else:
                raise ValueError(
                    f"root_repo '{root_repo}' is neither a filesystem path, "
                    f"a valid URL, nor an existing remote name"
                )

        if self._remote_exists(remote_name):
            self._git(["remote", "set-url", remote_name, resolved])
        else:
            self._git(["remote", "add", remote_name, resolved])

        return remote_name

    def _remote_exists(self, name: str) -> bool:
        r = self._git_quiet(["remote", "get-url", name], check=False)
        return r.returncode == 0

    def _sanitize_ref_component(self, name: str) -> str:
        """
        Convert an arbitrary snapshot name into a valid single ref path component.

        - replaces whitespace with '-'
        - removes disallowed characters
        - avoids forbidden sequences
        """
        name = name.strip()
        name = re.sub(r"\s+", "-", name)  # spaces -> -
        name = re.sub(r"[^A-Za-z0-9._-]+", "-", name)  # keep safe chars
        name = re.sub(r"-{2,}", "-", name).strip("-.")  # tidy

        if not name:
            name = "snapshot"

        # Disallow some ref edge cases
        forbidden = (
            name.startswith(".")
            or name.endswith(".")
            or name.endswith(".lock")
            or ".." in name
            or "@{" in name
        )
        if forbidden:
            name = re.sub(r"\.+", ".", name).strip(".")
            name = name.replace("@{", "-")
            if not name or name.endswith(".lock"):
                name = "snapshot"

        return name

    def _has_git_dir_here(self) -> bool:
        """
        True if working_dir has its own .git (directory or gitfile).
        This is the key signal that a repo is rooted here, not only in a parent.
        """
        git_path = self.working_dir / ".git"
        return git_path.exists()

    def _pin_repo_env(self) -> None:
        """
        Force git to use ONLY this repo (no parent discovery).
        Supports normal repos and gitdir 'gitfile' (e.g., worktrees/submodules).
        """
        git_path = self.working_dir / ".git"
        self._env = os.environ.copy()
        self._env["GIT_WORK_TREE"] = str(self.working_dir)

        if git_path.is_file():
            # .git is a "gitfile": contains "gitdir: /actual/path"
            content = git_path.read_text(encoding="utf-8", errors="replace").strip()
            prefix = "gitdir:"
            if not content.lower().startswith(prefix):
                raise RuntimeError(f"Invalid .git file at {git_path}")
            gitdir = content[len(prefix) :].strip()
            gitdir_path = (self.working_dir / gitdir).resolve()
            self._env["GIT_DIR"] = str(gitdir_path)
        else:
            # normal .git directory
            self._env["GIT_DIR"] = str(git_path)

        # Also prevent upward discovery if something goes odd
        self._env["GIT_CEILING_DIRECTORIES"] = str(self.working_dir)

    def _assert_repo_root_is_working_dir(self) -> None:
        """
        Verify that git sees the repo root as working_dir.
        This catches the case where we accidentally target a parent repo.
        """
        # With pinned env, this should always match working_dir
        result = self._git_capture(["rev-parse", "--show-toplevel"], check=False)
        if result.returncode != 0:
            raise RuntimeError(f"Git repo check failed: {result.stderr.strip()}")

        toplevel = Path(result.stdout.strip()).resolve()
        if toplevel != self.working_dir:
            raise RuntimeError(
                f"Refusing to use repo rooted at {toplevel}; expected {self.working_dir}. "
                f"This usually means a parent repo is being picked up."
            )

    def _git_raw(self, args):
        """
        Git calls before repo pinning. We still prevent parent discovery by ceiling.
        """
        env = os.environ.copy()
        env["GIT_CEILING_DIRECTORIES"] = str(self.working_dir)
        self._git_run_env(args, env=env, check=True)

    def _git(self, args):
        """
        Git calls pinned to this repo (no parent discovery possible).
        """
        self._git_run(args, check=True)

    def _head_hash(self, allow_none: bool = False) -> str | None:
        result = self._git_capture(["rev-parse", "HEAD"], check=False)
        if result.returncode != 0:
            if allow_none:
                return None
            raise RuntimeError("HEAD does not exist")
        return result.stdout.strip()

    def _snapshot_ref(self, name: str) -> str:
        return f"{SNAPSHOT_REF_PREFIX}/{name}"

    def _git_capture(self, args: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
        return self._git_run(args, check=check, capture=True)

    def _git_quiet(self, args: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
        return self._git_run(args, check=check)

    def _git_run(
        self,
        args: list[str],
        *,
        check: bool = True,
        capture: bool = False,
        passthrough: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        if capture:
            stdout = subprocess.PIPE
            stderr = subprocess.PIPE
        elif passthrough:
            stdout = None
            stderr = None
        else:
            stdout = subprocess.DEVNULL
            stderr = subprocess.DEVNULL
        return self._git_run_env(
            args,
            env=self._env,
            check=check,
            stdout=stdout,
            stderr=stderr,
        )

    def _git_run_env(
        self,
        args: list[str],
        *,
        env: dict[str, str],
        check: bool,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    ) -> subprocess.CompletedProcess[str]:
        try:
            return subprocess.run(
                ["git"] + args,
                cwd=self.working_dir,
                env=env,
                check=check,
                stdout=stdout,
                stderr=stderr,
                text=True,
            )
        except subprocess.CalledProcessError as exc:
            cmd = " ".join(exc.cmd) if isinstance(exc.cmd, list) else str(exc.cmd)
            parts = [f"git failed: {cmd}"]
            if exc.stdout:
                parts.append(str(exc.stdout).strip())
            if exc.stderr:
                parts.append(str(exc.stderr).strip())
            raise RuntimeError("\n".join(parts)) from exc

    def _write_extra_gitignore(self, patterns: Iterable[str]) -> None:
        """
        Adds ignore patterns in .git/info/exclude (applies in addition to .gitignore).
        """
        exclude = self.working_dir / ".git" / "info" / "exclude"
        exclude.parent.mkdir(parents=True, exist_ok=True)

        existing = set()
        if exclude.exists():
            existing = {
                line.strip()
                for line in exclude.read_text(
                    encoding="utf-8", errors="replace"
                ).splitlines()
                if line.strip()
            }

        new_lines = []
        for p in patterns:
            p = p.strip()
            if p and p not in existing:
                new_lines.append(p)

        if new_lines:
            with exclude.open("a", encoding="utf-8") as f:
                f.write("\n")
                for line in new_lines:
                    f.write(line + "\n")
