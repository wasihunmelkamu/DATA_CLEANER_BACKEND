import { Router } from "express";
import { naturalLanguageQueryController } from "../controllers/naturalLanguageQuery.controller.js";

const router = Router()

router.post("/", naturalLanguageQueryController)

export default router