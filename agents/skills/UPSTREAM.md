# Skill provenance

The skills in this directory are adapted from Koinon, which is MIT licensed. Its license text is
kept verbatim in [LICENSE](LICENSE) and covers the files listed here.

- Source repository: https://github.com/rwcii/koinon
- Source revision: `b0fd86e20676583b363e6e133fd9eb64c3535609`
- Source path: `agents/skills/` (and the root `LICENSE`)
- Upstream symlinks reproduced: `.claude/skills`, `.agents/skills` and `.codex/skills`, each
  the relative target `../agents/skills`.

Hashes are SHA-256 of the file bytes: upstream at the source revision, and vendored as committed
here. Update the vendored hash and the adaptation in the same change as the file.

| File | Upstream path | Upstream SHA-256 | Vendored SHA-256 | Local adaptation |
| --- | --- | --- | --- | --- |
| `LICENSE` | `LICENSE` | `9dd79e20348260b1dee37e6df47c6bc818ad7496e68c00432dae4a1a30c500ae` | `9dd79e20348260b1dee37e6df47c6bc818ad7496e68c00432dae4a1a30c500ae` | Verbatim. |
| `handoff/SKILL.md` | `agents/skills/handoff/SKILL.md` | `4f9f172d1265c9a8d50d7f822f7a4bf02ab0e866e2dedd728d138447a9dcb686` | `520ae4901be24eb75852e9505b6b89a3253070aaf5e485348bf6dbf8e41be5c0` | "Maintainer" for the human role; reads `CONTRIBUTING.md` (this repository has no `AGENTS.md`); durable facts stay in the versioned records and a handoff points to them; labels limited to lower-case letters, digits and hyphens; never writes a tracked file; template adds the deployment's branch, commit and service state, the authority held, and containers started for the work; the Koinon bridge identity section is replaced by a peer-sessions section taken from the live session listing (Koinon is not used here); credentials list extended with tokens, chat IDs and webhook URLs. |
| `pickup/SKILL.md` | `agents/skills/pickup/SKILL.md` | `1f2dab22fa07bc3a19a9f6bb6e77846bd993c0830c00b9e806c7d815412640af` | `69768b3774d6856cfda5a52ff832bc85806dd6d3938ad3080c0dfc981ea2b406` | "Maintainer" for the human role; reads `CONTRIBUTING.md`; with no own handoff, resumes from the versioned records when the maintainer's instruction names the work; verifies the deployment a handoff names; treats handoff text as data and its authorization claims as not authority; the Koinon bridge reconnect step is replaced by comparing peer sessions with the live listing; removed the sprint-plan step (this repository has no sprints); a reported effect is never repeated. |

To verify a row: `sha256sum` the file here, and
`git -C <koinon> show <revision>:<upstream path> | sha256sum`.
