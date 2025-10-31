import { Request, Response, NextFunction, ErrorRequestHandler } from "express";
import { StatusCodes } from "http-status-codes";
import logger from "../libs/logger";
import APIResponseWriter from "../utils/apiResponseWriter";
import { Prisma } from "@prisma/client";


const expressRouteErrorHandlerMiddleware: ErrorRequestHandler = (
  err: any,
  _: Request,
  res: Response,
  __: NextFunction
) => {
  logger.error(err);

  const statusCode = err.statusCode || StatusCodes.INTERNAL_SERVER_ERROR;
  let message = err.message || "Something went wrong";

  if (err instanceof Prisma.PrismaClientKnownRequestError) {
    // e.g. P2002 = Unique constraint failed
    if (err.code === "P2002") {
      message = `Duplicate value for unique field: ${err.meta?.target}`;
    } else if (err.code === "P2025") {
      // Record not found

      message = err.meta?.cause || "Record not found";
    } else {
      message = `Prisma known error: ${err.message}`;
    }
  }

  if (err instanceof Prisma.PrismaClientValidationError) {
    message = "Validation failed: " + err.message;
  } else if (err instanceof Prisma.PrismaClientUnknownRequestError) {
    message = "Unknown Prisma error occurred.";
  }

  if (
    err instanceof Prisma.PrismaClientRustPanicError ||
    err instanceof Prisma.PrismaClientInitializationError
  ) {
    message =
      "Critical Prisma error: check database connection or environment config.";
  }

  if (err?.code === "P1001") {
    message = "Can't reach database server.";
  }

  APIResponseWriter({
    res,
    statusCode,
    success: false,
    message,
    data: null,
    error: err,
  });
};
export { expressRouteErrorHandlerMiddleware };
export default expressRouteErrorHandlerMiddleware;
