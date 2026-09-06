import * as fs from "node:fs";
import { allMailboxCases, mailboxCaseCells } from "./mailbox-cases.js";

const file = new URL("../../docs/mailbox-guarantees.md", import.meta.url);
const doc = fs.readFileSync(file, "utf8");
const header = "| ID | Category | Test file | Test name | Envelope kind | Boundary | Persistence mode | Assertion | Assumptions | Gap removal condition |";
const start = doc.indexOf(header);
if (start < 0) throw new Error("Mailbox matrix header not found");
const lines = [header, "|---|---|---|---|---|---|---|---|---|---|",
	...allMailboxCases.map((row) => `| ${mailboxCaseCells(row).join(" | ")} |`)];
fs.writeFileSync(file, doc.slice(0, start) + lines.join("\n") + "\n");
