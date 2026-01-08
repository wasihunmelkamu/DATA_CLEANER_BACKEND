import { Router } from "express";
import { checkHealthController } from "../controllers/health.controller.js";

const router = Router();

router.get("/", checkHealthController);

export default router;
