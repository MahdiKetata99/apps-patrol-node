// Builds dist/esm (ES modules) and dist/cjs (CommonJS), each with type declarations.
import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const local = join(root, "node_modules", "typescript", "bin", "tsc");
const hoisted = join(root, "..", "..", "node_modules", "typescript", "bin", "tsc");
const tsc = existsSync(local) ? local : hoisted;

rmSync(join(root, "dist"), { recursive: true, force: true });
const run = (...args) => execFileSync(process.execPath, [tsc, "-p", join(root, "tsconfig.json"), ...args], { stdio: "inherit", cwd: root });

run();
run("--module", "commonjs", "--moduleResolution", "bundler", "--outDir", join(root, "dist", "cjs"));

mkdirSync(join(root, "dist", "cjs"), { recursive: true });
writeFileSync(join(root, "dist", "cjs", "package.json"), JSON.stringify({ type: "commonjs" }) + "\n");
