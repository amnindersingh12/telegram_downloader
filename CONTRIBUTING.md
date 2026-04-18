# Contributing to TGrab

We love contributions! Since this is a high-performance streaming tool, please follow these guidelines:

## Code Style

- **Python**: Use `black` formatting. Keep logic in `core.py` and API in `app.py`.
- **JS**: Vanilla JS only. No heavy frameworks (React/Vue). We prioritize raw DOM performance for the virtual scroller.
- **CSS**: Modern CSS (Flexbox/Grid). Avoid Tailwind unless for specific utilities.

## Local Development

1. Fork and clone the repo.
2. Install dependencies: `pip install -r requirements.txt`.
3. Run with `python app.py`.

## Pull Requests

1. Create a feature branch.
2. Ensure your changes don't break the Telethon session handling.
3. Keep PRs small and focused.

---

*“Speed is a feature.”*
