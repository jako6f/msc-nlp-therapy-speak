# Code Quality Bar

- Write for a social science MSc reader and a political science methods marker: clear, direct, and easy to inspect.
- Keep notebooks as readable analysis documents, not production systems: prefer simple sequential code over custom helpers, abstractions, or mini-frameworks.
- Use a small global setup cell only for imports, paths, display options, and checks that apply to the whole notebook.
- Put section-specific parameters in short config cells immediately above the code that uses them, with plain-English comments explaining what they control.
- Make every important filter, exclusion, data restriction, and selection rule explicit in code and briefly justified in nearby markdown.
- Write markdown for an external reader: no internal workflow language, no references to private plans, and no unexplained decision jumps.
- Preserve user-made wording, comment, and markup refinements unless explicitly asked to revise them; do not overwrite the authorial voice during later edits.
- Use first-person singular where an authorial voice is needed; avoid first-person plural.
- Add concise Python comments that explain the conceptual reason for tricky steps, not just the mechanics. Err on the side of many vs few.
- Use functions only when they genuinely reduce repetition or clarify logic; keep any functions short and transparent.
- Choose descriptive variable names that reflect the substance of the analysis, such as `df_topic_scope` or `final_topic_summary`.
- Ground consequential methodological choices in the relevant literature when appropriate, with citations integrated where the choice is introduced.
- Keep outputs reproducible and report-ready: save final/interim datasets deliberately, display compact handoff summaries, and avoid hidden side effects.
