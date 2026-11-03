# Agent skills

Skills shared by every coding agent that works on this repository. Claude Code and Codex read
them through relative symlinks (`.claude/skills`, `.agents/skills`, `.codex/skills` →
`../agents/skills`), so there is one copy of each. Provenance and adaptations are in
[UPSTREAM.md](UPSTREAM.md).

| Skill | Use it to |
| --- | --- |
| [handoff](handoff/SKILL.md) | Write a snapshot of this agent's work to its own ignored `_handoff/<agent>/` directory at the end of a session or before a risky operation. |
| [pickup](pickup/SKILL.md) | Resume from this agent's newest handoff and verify it against the live repository and deployment. |

Run one in Claude Code with `/<name>` (for example `/handoff`), in Codex with `$<name>`, or tell
any other agent: "Read `agents/skills/<name>/SKILL.md` and follow it."

Handoffs are session state: each agent writes its own `_handoff/<agent>/` directory in the main
checkout, which Git ignores, and nothing there is ever staged, committed or pushed.
