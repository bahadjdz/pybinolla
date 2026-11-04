# Contributing to pybinolla

Thank you for your interest in contributing to **pybinolla**! We welcome bug reports, feature requests, documentation improvements, and pull requests.

## How to Contribute

### 1. Reporting Bugs
- Search existing [Issues](https://github.com/bahadjdz/pybinolla/issues) before opening a new one.
- Use the **Bug Report** template and provide minimal reproducible code.
- Never share real account credentials, passwords, or session tokens in public issues.

### 2. Suggesting Features
- Open a feature request in [Issues](https://github.com/bahadjdz/pybinolla/issues).
- Describe the use case and why it benefits the community.

### 3. Pull Requests
1. Fork the repository and create your branch from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```
2. Install the package in editable mode with dev dependencies:
   ```bash
   pip install -e ".[dev]"
   ```
3. Ensure all tests and style checks pass:
   ```bash
   python -m py_compile pybinolla/*.py
   ```
4. Commit your changes with clear commit messages following Conventional Commits (e.g. `feat:`, `fix:`, `docs:`).
5. Push to your branch and submit a Pull Request.

## Code Style
- Follow PEP 8 guidelines.
- Use explicit type annotations where appropriate.
- Keep asynchronous code clean and properly structured with `asyncio`.

## License
By contributing to pybinolla, you agree that your contributions will be licensed under the project's [MIT License](LICENSE).

