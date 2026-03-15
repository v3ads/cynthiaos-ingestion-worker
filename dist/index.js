"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = __importDefault(require("express"));
const app = (0, express_1.default)();
const PORT = parseInt(process.env.PORT ?? "3001", 10);
const SERVICE_NAME = "cynthiaos-ingestion-worker";
app.use(express_1.default.json());
// ── Health check ──────────────────────────────────────────────────────────────
app.get("/health", (_req, res) => {
    res.status(200).json({
        service: SERVICE_NAME,
        status: "ok",
        timestamp: new Date().toISOString(),
    });
});
// ── Catch-all ─────────────────────────────────────────────────────────────────
app.use((_req, res) => {
    res.status(404).json({ error: "not_found" });
});
// ── Start ─────────────────────────────────────────────────────────────────────
app.listen(PORT, "0.0.0.0", () => {
    console.log(`[${SERVICE_NAME}] listening on port ${PORT}`);
});
exports.default = app;
//# sourceMappingURL=index.js.map