import { Router } from "express";
import {
  cleanupTableDataController,
  cleanupTableDataControllerTest,
  dmsUsersNameCapitalizerController,
  getDuplicateEntitiesByFullNameController,
  getDuplicateEntitiesByNameWithTypeController,
} from "../controllers/dataCleanUp.controller";

import {
  getEntitiesWithGivenNameController,
  analyzeDuplicateEntitiesController,
} from "../controllers/entitiesMerge.controller";
import { applyEntitiesDuplicateMergeController, applyEntitiesDuplicateMergeManuallyController } from "../controllers/applyEntitiesDuplicateMerge.controller";

const router = Router();
router.post("/", cleanupTableDataController);
router.post("/test", cleanupTableDataControllerTest);
router.post("/dms/capitalize-names", dmsUsersNameCapitalizerController);

router.get(
  "/entities/similar/by-name",
  getDuplicateEntitiesByFullNameController
);

// ! Updated once
router.get(
  "/entities/similar/by-name/:type",
  getDuplicateEntitiesByNameWithTypeController
);

router.get(
  "/entities/by-name/:name",
  getEntitiesWithGivenNameController
);


router.post(
  "/entities/duplicates/analyze",
  analyzeDuplicateEntitiesController
);

router.post(
  "/entities/resolve-duplicates",
  applyEntitiesDuplicateMergeController
);

router.post(
  "/entities/resolve-duplicates/manual",
  applyEntitiesDuplicateMergeManuallyController
);

// !


export default router;
