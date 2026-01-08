import { createServer } from "http";
import app from "./app.js";
import { PORT } from "./config/env";
import logger from "./libs/logger";
// import prisma from "./config/db";
import { registerSignalHandlers } from "./utils/signalHandler";
function startApp() {
  const server = createServer(app);
  server.listen(PORT, () => {
    logger.info(`🚀Server running at http://localhost:${PORT}`);
  });

  // Register shutdown hooks
  // registerSignalHandlers(server, prisma);
}

startApp();
