
import subprocess
import sys
from pathlib import Path
from rich.console import Console
from rich.markdown import Markdown

console = Console()

LANGUAGE_MAP = {
    ".py": ("python", "#"),
    ".js": ("javascript", "//"),
    ".ts": ("typescript", "//"),
    ".java": ("java", "//"),
    ".cs": ("csharp", "//"),
    ".cpp": ("cpp", "//"),
    ".cc": ("cpp", "//"),
    ".c": ("c", "//"),
    ".rb": ("ruby", "#"),
    ".go": ("go", "//"),
    ".php": ("php", "//"),
    ".rs": ("rust", "//"),
    ".sh": ("bash", "#"),
    ".swift": ("swift", "//"),
    ".tf": ("terraform", "#"),
    ".tfvars": ("terraform", "#"),
    ".groovy": ("groovy", "//"),
    ".yaml": ("yaml", "#"),
    ".yml": ("yaml", "#"),
    ".json": ("json", "//"),
    ".tpl": ("helm", "#")
}

SPECIAL_NAMES = {
    "Dockerfile": ("docker", "#"),
    "Jenkinsfile": ("groovy", "//"),
    "Chart.yaml": ("helm", "#")
}

DEFAULT_MODEL = "codellama:7b-instruct"

MODEL_MAP = {
    "terraform": "deepseek-coder",
    "helm": "deepseek-coder",
    "yaml": "qwen2.5-coder:7b",
    "json": "qwen2.5-coder:7b",
    "docker": "starcoder",
    "groovy": "starcoder"
}

def detect_language_and_comment(filepath: Path):
    if filepath.name in SPECIAL_NAMES:
        return SPECIAL_NAMES[filepath.name]
    ext = filepath.suffix.lower()
    return LANGUAGE_MAP.get(ext, ("plaintext", "//"))

def get_model_for_language(language: str):
    return MODEL_MAP.get(language, DEFAULT_MODEL)

def run_ollama_prompt(model: str, prompt: str) -> str:
    try:
        result = subprocess.run(
            ["ollama", "run", model],
            input=prompt.encode("utf-8"),
            capture_output=True,
            timeout=240
        )
        if result.returncode != 0:
            raise Exception(result.stderr.decode())
        return result.stdout.decode("utf-8").strip()
    except Exception as e:
        return f"Error running Ollama model '{model}': {e}"

def generate_prompt(code: str, language: str, filename: str) -> str:
    if language == "terraform":
        return f"""
You are a Terraform expert. Review the file '{filename}'.

1. Identify potential security risks (e.g., open ingress, hardcoded secrets).
2. Suggest performance or reliability improvements.
3. Recommend module or variable best practices.
4. Wrap Terraform code in triple backticks with `hcl`.

```hcl
{code}
```
"""
    elif language == "docker":
        return f"""
You are a Docker expert. Review the Dockerfile '{filename}'.

1. Suggest improvements to layering, caching, image size, and security.
2. Identify common anti-patterns or inefficiencies.
3. Ensure best practices are followed.
4. Wrap Dockerfile content in triple backticks.

```
{code}
```
"""
    elif language == "yaml":
        return f"""
You are a Kubernetes and DevOps expert. Review the YAML file '{filename}'.

1. Check for valid API versions, missing probes, resource limits, and security context.
2. Suggest improvements for production-readiness and reliability.
3. Wrap YAML in triple backticks.

```yaml
{code}
```
"""
    elif language == "helm":
        return f"""
You are a Helm chart reviewer. Review the Helm template '{filename}'.

1. Check for correct templating practices and values usage.
2. Suggest improvements to structure, naming, or defaults.
3. Wrap Helm YAML/template in triple backticks.

```yaml
{code}
```
"""
    elif language == "groovy":
        return f"""
You are a DevOps pipeline engineer. Review the Groovy pipeline script '{filename}'.

1. Identify fragile logic, hardcoded paths, missing error handling.
2. Suggest improvements for reliability and maintainability.
3. Use triple backtick code blocks with `groovy`.

```groovy
{code}
```
"""
    else:
        return f"""
You are a senior software engineer and expert code reviewer. Review the following {language} code in the file '{filename}'.

1. Detect bugs, logic errors, or bad practices.
2. Provide concrete fixes using correct {language} syntax in BEFORE/AFTER format.
3. Use markdown code blocks with the language identifier `{language}`.

```{language}
{code}
```
"""

def save_markdown_review(review_text: str, filepath: Path):
    output_path = filepath.with_suffix(filepath.suffix + ".review.md")
    output_path.write_text(review_text, encoding="utf-8")
    console.print(f"[green]Saved review markdown to:[/green] {output_path}")

def review_code_file(filepath: Path):
    language, _ = detect_language_and_comment(filepath)
    model = get_model_for_language(language)
    code = filepath.read_text(encoding="utf-8")
    prompt = generate_prompt(code, language, filepath.name)

    console.print(f"[bold green]Reviewing:[/bold green] {filepath} as [bold yellow]{language}[/bold yellow] using model [bold]{model}[/bold]...\n")
    response = run_ollama_prompt(model, prompt)

    save_markdown_review(response, filepath)
    console.print(Markdown("### 🧠 AI Review Output\n"))
    console.print(Markdown(response))

def get_target_files(paths):
    all_files = []
    for path_str in paths:
        path = Path(path_str)
        if path.is_file():
            all_files.append(path)
        elif path.is_dir():
            all_files.extend([f for f in path.rglob("*") if f.suffix.lower() in LANGUAGE_MAP or f.name in SPECIAL_NAMES])
        else:
            console.print(f"[yellow]Skipping unknown path:[/yellow] {path}")
    return all_files

if __name__ == "__main__":
    if len(sys.argv) < 2:
        console.print("[red]Usage:[/red] python qa_agent_recursive.py file_or_dir1 [file_or_dir2 ...]")
        sys.exit(1)

    files = get_target_files(sys.argv[1:])
    for file in files:
        try:
            review_code_file(file)
        except Exception as e:
            console.print(f"[red]Error reviewing {file}: {e}[/red]")
