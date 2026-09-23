import os
from pathlib import Path
from typing import Optional, Tuple

import jpype


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent

JARS_DIR = Path(os.environ.get("JARS_DIR", BASE_DIR / "jars"))
PPRJ_DIR = Path(os.environ.get("PPRJ_DIR", BASE_DIR / "resources"))

env_pprj_file = os.environ.get("PPRJ_FILE")
if env_pprj_file:
    PPRJ_FILE = Path(env_pprj_file)
else:
    PPRJ_FILE = PPRJ_DIR / "essential_baseline_6_20.pprj"


# Protégé access mode:
#
#   file   = open the .pprj directly
#   server = connect to Protégé Server
#
PROTEGE_MODE = os.environ.get("PROTEGE_MODE", "file").strip().lower()

PROTEGE_SERVER = os.environ.get(
    "PROTEGE_SERVER",
    "",
)

PROTEGE_PROJECT = os.environ.get(
    "PROTEGE_PROJECT",
    "",
)

PROTEGE_USERNAME = os.environ.get(
    "PROTEGE_USERNAME",
    "",
)

PROTEGE_PASSWORD = os.environ.get(
    "PROTEGE_PASSWORD",
    "",
)

PROTEGE_POLL_EVENTS = (
    os.environ.get("PROTEGE_POLL_EVENTS", "true").strip().lower()
    in ("1", "true", "yes", "on")
)


JARS_DIR.mkdir(parents=True, exist_ok=True)
PPRJ_DIR.mkdir(parents=True, exist_ok=True)

# Collect all JAR files placed in the jars directory.
CLASSPATH = [str(jar.resolve()) for jar in JARS_DIR.glob("*.jar")]


# ---------------------------------------------------------------------------
# JVM
# ---------------------------------------------------------------------------

def start_jvm() -> None:
    """Ensure the JVM is running with the configured classpath."""

    if jpype.isJVMStarted():
        return

    if not CLASSPATH:
        raise RuntimeError(
            f"No .jar files found in {JARS_DIR}. "
            "Add the Protégé JARs before starting the JVM."
        )

    print("Starting JVM...")
    jpype.startJVM(classpath=CLASSPATH)
    print("JVM started successfully!")


_PROTEGE_PROJECT: Optional[object] = None
_KNOWLEDGE_BASE: Optional[object] = None


# ---------------------------------------------------------------------------
# Project loading
# ---------------------------------------------------------------------------

def load_pprj() -> Optional[Tuple[object, object]]:
    """
    Load the configured Protégé project.

    file mode:
        Open the configured .pprj directly.

    server mode:
        Connect to the project hosted by Protégé Server.
    """

    try:
        global _PROTEGE_PROJECT, _KNOWLEDGE_BASE  # noqa: PLW0603

        if _KNOWLEDGE_BASE is not None:
            return _PROTEGE_PROJECT, _KNOWLEDGE_BASE

        start_jvm()

        if PROTEGE_MODE == "server":

            if not PROTEGE_SERVER:
                raise RuntimeError(
                    "PROTEGE_SERVER must be set when "
                    "PROTEGE_MODE=server"
                )

            if not PROTEGE_PROJECT:
                raise RuntimeError(
                    "PROTEGE_PROJECT must be set when "
                    "PROTEGE_MODE=server"
                )

            if not PROTEGE_USERNAME:
                raise RuntimeError(
                    "PROTEGE_USERNAME must be set when "
                    "PROTEGE_MODE=server"
                )

            print(
                f"Connecting to Protégé Server "
                f"{PROTEGE_SERVER}, "
                f"project '{PROTEGE_PROJECT}'..."
            )

            remote_project_manager = jpype.JClass(
                "edu.stanford.smi.protege.server.RemoteProjectManager"
            ).getInstance()

            project = remote_project_manager.getProject(
                PROTEGE_SERVER,
                PROTEGE_USERNAME,
                PROTEGE_PASSWORD,
                PROTEGE_PROJECT,
                PROTEGE_POLL_EVENTS,
            )

            if project is None:
                raise RuntimeError(
                    "Protégé Server returned no project."
                )

            print(
                f"Connected to remote Protégé project "
                f"'{PROTEGE_PROJECT}'."
            )

        elif PROTEGE_MODE == "file":

            if not PPRJ_FILE.exists():
                print(
                    f"Error: The file {PPRJ_FILE} was not found!"
                )
                return None

            protege_package = jpype.JPackage(
                "edu.stanford.smi.protege.model"
            )
            project_class = protege_package.Project

            print(
                f"Loading local Protégé project: {PPRJ_FILE}..."
            )

            project = project_class.loadProjectFromFile(
                str(PPRJ_FILE),
                [],
            )

        else:
            raise RuntimeError(
                f"Unsupported PROTEGE_MODE '{PROTEGE_MODE}'. "
                "Use 'file' or 'server'."
            )

        kb = project.getKnowledgeBase()

        _PROTEGE_PROJECT = project
        _KNOWLEDGE_BASE = kb

        print(
            f"Knowledge Base loaded successfully "
            f"(mode={PROTEGE_MODE})."
        )

        return project, kb

    except Exception as exc:  # pylint: disable=broad-except
        print(f"Error while loading project: {exc}")
        return None


def get_knowledge_base() -> Optional[object]:
    """Return the loaded Knowledge Base, if available."""
    return _KNOWLEDGE_BASE


def get_project() -> Optional[object]:
    """Return the loaded Protégé project, if available."""
    return _PROTEGE_PROJECT


# ---------------------------------------------------------------------------
# Publishing
# ---------------------------------------------------------------------------

def call_publish_async(
    project: object,
    url: str,
    user: str,
    pwd: str,
) -> Optional[str]:
    """Trigger asynchronous publishing via PublishService."""

    try:
        start_jvm()

        publish_service = jpype.JClass(
            "com.enterprise_architecture."
            "essential_os.publish_service.PublishService"
        )

        job_id = publish_service.startPublishAsync(
            project,
            url,
            user,
            pwd,
        )

        return str(job_id)

    except Exception as exc:  # pylint: disable=broad-except
        print(f"Error while starting publish job: {exc}")
        return None


def get_publish_status(
    job_id: str,
) -> Optional[Tuple[str, str]]:
    """Retrieve the status and logs of a publish job."""

    try:
        start_jvm()

        publish_service = jpype.JClass(
            "com.enterprise_architecture."
            "essential_os.publish_service.PublishService"
        )

        status = publish_service.getPublishStatus(job_id)
        logs = publish_service.getPublishLogs(job_id)

        return str(status), str(logs)

    except Exception as exc:  # pylint: disable=broad-except
        print(f"Error while checking publish status: {exc}")
        return None


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def save_project():
    """
    Persist project changes.

    In file mode the API owns the local Protégé project and therefore
    explicitly saves it.

    In server mode changes are made directly against the remote
    Protégé knowledge base. Protégé Server owns persistence and its
    autosave mechanism writes the repository to disk.
    """

    project = get_project()

    if project is None:
        print("Cannot save project: Protégé project not loaded.")
        return False, ["Protégé project not loaded."]

    if PROTEGE_MODE == "server":

        print(
            "Server mode: change is already in the remote "
            "Protégé knowledge base; server owns persistence."
        )
        return True, None

    try:
        start_jvm()

        ArrayList = jpype.JClass("java.util.ArrayList")
        errors = ArrayList()

        save_method = getattr(project, "save", None)

        if callable(save_method):
            project.save(errors)

        else:
            legacy_save = getattr(
                project,
                "saveProject",
                None,
            )

            if callable(legacy_save):
                legacy_save()
            else:
                print(
                    "Project object does not expose "
                    "a save/saveProject method."
                )
                return False, [
                    "Project object does not expose "
                    "a save/saveProject method."
                ]

        if hasattr(errors, "isEmpty") and not errors.isEmpty():
            error_messages = [
                str(err) for err in errors
            ]

            print(
                f"Errors while saving project: "
                f"{error_messages}"
            )

            return False, error_messages

        print("Protégé project saved successfully.")
        return True, None

    except Exception as exc:  # pylint: disable=broad-except
        print(f"Error while saving project: {exc}")
        return False, [
            f"Error while saving project: {exc}"
        ]
