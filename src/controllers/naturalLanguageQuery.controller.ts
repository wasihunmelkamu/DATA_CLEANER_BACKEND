// controllers/naturalLanguageQuery.controller.ts


import { StatusCodes } from "http-status-codes";
import { entitiesPrisma, dmsPrisma } from "../config/db.js";
import APIResponseWriter from "../utils/apiResponseWriter.js";
import expressAsyncWrapper from "../utils/asyncHandler.js";
import RouteError from "../utils/routeErrors.js";
import logger from "../libs/logger.js";
import { DataSourceRouterService, RoutingDecision } from "../services/dataSourceRouter.service.js";
import { NaturalLanguageQueryAIService } from "../services/naturalLanguageQueryAIService.service.js";
import { NaturalLanguageQueryAIServiceDMS } from "../services/naturalLanguageQueryAIService.dms.service.js";
import { MarkdownSummaryAIService } from "../services/markdownSummaryAI.service.js";

// utils/extractDatabaseError.ts
export function extractDatabaseError(error: any): string {
    if (error?.meta?.message) return error.meta.message; // Prisma known error
    if (error?.message) return error.message.split("\n")[0]; // Clean up stack traces
    return "Unknown database error";
}

// === Types ===
interface NaturalQueryRequestBody {
    question: string;
    limit?: number; // optional
}

// Maximum retries after execution failure
const MAX_CORRECTION_ATTEMPTS = 2;

export const naturalLanguageQueryController = expressAsyncWrapper(
    async (req, res) => {
        
        const { question }: NaturalQueryRequestBody = req.body;

        if (!question || typeof question !== "string" || !question.trim()) {
            throw RouteError.BadRequest("A valid 'question' string is required");
        }

        const cleanedQuestion = question.trim();
        const routingService = new DataSourceRouterService();
        const routingDecision: RoutingDecision = await routingService.routeQuestion(cleanedQuestion);

        logger.info("Routing Decision Made", {
            question: cleanedQuestion,
            routingDecision,
            userId: (req as any).user?.id || null,
        });

        console.log({routingDecision})



        // ✅ SHORT-CIRCUIT: If general question, return markdown immediately
        if (routingDecision.target === "general" && routingDecision.markdownResponse) {
            return APIResponseWriter({
                res,
                message: "General question answered",
                statusCode: StatusCodes.OK,
                success: true,
                data: {
                    markdown: routingDecision.markdownResponse,
                },
            });
        }

        // ❌ Handle edge case: general without markdown (shouldn’t happen)
        if (routingDecision.target === "general") {
            throw RouteError.BadRequest("General question detected but no response generated.");
        }

        // ✅ Handle unknown target
        if (routingDecision.target === "unknown") {
            throw RouteError.BadRequest(
                routingDecision.reason || "Cannot determine which system contains this data. Please clarify your question."
            );
        }

        // Choose correct AI service and DB client
        const aiService =
            routingDecision.target === "entities"
                ? new NaturalLanguageQueryAIService()
                : new NaturalLanguageQueryAIServiceDMS();

        const prismaClient = routingDecision.target === "entities" ? entitiesPrisma : dmsPrisma;

        let queryPlan = await aiService.generateQueryPlan(cleanedQuestion);
        let finalSql = queryPlan.sql;
        let attempts = 0;
        const errorFeedbackLog: Array<{ sql: string; error: string }> = [];

        while (attempts <= MAX_CORRECTION_ATTEMPTS) {
            try {
                // Apply limit if allowed
                let sqlToRun = finalSql;
                if (queryPlan.allowsLimit && req.body.limit) {
                    const limit = Math.min(Number(req.body.limit), 100);
                    sqlToRun = sqlToRun.replace(/\bLIMIT \?/i, `LIMIT ${limit}`);
                }

                const rawResults = await prismaClient.$queryRawUnsafe<any[]>(sqlToRun);
                const results = Array.isArray(rawResults) ? rawResults : [];

                logger.info("Query Executed Successfully", {
                    question: cleanedQuestion,
                    sql: sqlToRun,
                    resultCount: results.length,
                    attempts,
                    userId: (req as any).user?.id || null,
                });

                // ✅ Generate AI-powered Markdown summary
                const markdownService = new MarkdownSummaryAIService();
                const markdownSummary = await markdownService.generateSummary({
                    question: cleanedQuestion,
                    sql: sqlToRun,
                    results,
                    errorFeedback: errorFeedbackLog,
                    dataSource: routingDecision.target,
                });

                return APIResponseWriter({
                    res,
                    message: "Query executed successfully",
                    statusCode: StatusCodes.OK,
                    success: true,
                    data: {
                        markdown: markdownSummary,
                    },
                });
            } catch (error: any) {
                const dbError = extractDatabaseError(error);
                errorFeedbackLog.push({ sql: finalSql, error: dbError });

                logger.warn("SQL Execution Failed - Attempting AI Correction", {
                    attempt: attempts,
                    sql: finalSql,
                    error: dbError,
                    question: cleanedQuestion,
                });

                // Stop if max retries reached
                if (attempts === MAX_CORRECTION_ATTEMPTS) {
                    logger.error("Max correction attempts exceeded", { errorFeedbackLog });
                    break;
                }

                // Let AI correct based on real error
                const correctedPlan = await aiService.generateQueryPlan(cleanedQuestion, errorFeedbackLog);
                if (!correctedPlan.successStatus) {
                    logger.info("AI gave up after feedback", { reason: correctedPlan.explanation });
                    queryPlan = correctedPlan;
                    finalSql = correctedPlan.sql;
                    break;
                }

                queryPlan = correctedPlan;
                finalSql = correctedPlan.sql;
                attempts++;
            }
        }

        // ✅ FINAL FALLBACK: Still generate friendly Markdown even after failure
        const markdownServiceFallback = new MarkdownSummaryAIService();
        const fallbackMarkdown = await markdownServiceFallback.generateSummary({
            question: cleanedQuestion,
            sql: finalSql,
            results: [],
            errorFeedback: errorFeedbackLog,
            dataSource: routingDecision.target,
        });

        return APIResponseWriter({
            res,
            message: "Failed to execute query after multiple attempts.",
            statusCode: StatusCodes.BAD_REQUEST,
            success: false,
            data: {
                markdown: fallbackMarkdown,
            },
        });
    }
);