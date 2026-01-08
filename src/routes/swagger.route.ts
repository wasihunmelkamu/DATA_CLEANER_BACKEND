import fs from "fs";
import path from "path";
import { Router } from "express";
import swaggerUi from "swagger-ui-express";
import yaml from "js-yaml";
import { fileURLToPath } from "url";
import type { JsonObject } from "swagger-ui-express";
import logger from "../libs/logger";
import APIResponseWriter from "../utils/apiResponseWriter";
const __filename=fileURLToPath(import.meta.url)
const __dirname=path.dirname(__filename)
const SwaggerRouter = Router();

// Path to Swagger YAML file
const swaggerFilePath = path.join(__dirname, "../../docs/swagger.yaml");

function loadSwaggerSpec(): JsonObject | null {
  try {
    const fileContents = fs.readFileSync(swaggerFilePath, "utf8");
    const parsedYaml = yaml.load(fileContents);

    if (!parsedYaml || typeof parsedYaml !== "object") {
      throw new Error("Parsed Swagger YAML is invalid or empty.");
    }

    return parsedYaml as JsonObject;
  } catch (err) {
    logger.error("❌ Failed to load Swagger documentation.");
    logger.error(err);
    return null;
  }
}

const swaggerSpec = loadSwaggerSpec();

if (swaggerSpec) {
  SwaggerRouter.use("/", swaggerUi.serve, swaggerUi.setup(swaggerSpec));
} else {
  SwaggerRouter.use("/", (_, res) => {
    APIResponseWriter({
      res,
      success: false,
      statusCode: 500,
      message: "Check if the YAML file exists and is properly formatted.",
      error: new Error("Swagger documentation could not be loaded."),
    });
  });
}

export default SwaggerRouter;
