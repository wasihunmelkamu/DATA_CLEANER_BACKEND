import express from "express";
import cors from "cors";
import { fileURLToPath } from "url";
import expressRouteErrorHandlerMiddleware from "./middlewares/expressRouteErrorHandler.js";
import rateLimiter from "./middlewares/rateLimiter.js";

import { swaggerRoute, healthRoute, tablesRoute, cleanupRoute } from "./routes";
import path from "path";
import { HEALTH_CHECK_URL, NODE_ENV } from "./config/env.js";
import { startHealthCheckCron } from "./crons/index.js";
import morgan from "morgan";
import { logger } from "@azure/identity";
import { dmsPrisma, entitiesPrisma } from "./config/db.js";
import naturalQueryRoute from "./routes/natural-query.route.js";
const __filename=fileURLToPath(import.meta.url)
const __dirname=path.dirname(__filename)
const app = express();

app.use(cors());
app.use(express.json());
app.use(morgan("tiny"), (req, res, next) => {
  logger.info(`${req.method} ${req.path} ${res.statusCode}`);
  next();
});

// 🚦 Apply rate limiting to all incoming requests
app.use(rateLimiter);

// Serve static files from the 'public' directory
app.use(express.static(path.join(__dirname, "../public")));

// Serve index.html on root "/"
app.get("/", (_, res) => {
  res.sendFile(path.join(__dirname, "../public/index.html"));
});

// ✅ Swagger docs (at /docs)
app.use("/docs", swaggerRoute);

// ❤️ Health check endpoint (under versioned API namespace)
app.use("/api/v1/health", healthRoute);
app.use("/api/v1/tables", tablesRoute);
app.use("/api/v1/cleanup", cleanupRoute);
app.use("/api/v1/natural-query", naturalQueryRoute);

app.get("/test", async (_, res) => {
  const entities = await entitiesPrisma.entity.findMany({
    where: {},
    take: 10,
  });

  res.json({ entities });
});


if (NODE_ENV !== "development" && HEALTH_CHECK_URL) {
  startHealthCheckCron(HEALTH_CHECK_URL, true);
}

// 🧯 Global error handling middleware (handles thrown errors or rejected promises)
app.use(expressRouteErrorHandlerMiddleware);

export default app;
