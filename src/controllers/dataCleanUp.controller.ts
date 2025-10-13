import { StatusCodes } from "http-status-codes";
import { dmsPrisma, entitiesPrisma } from "../config/db";
import APIResponseWriter from "../utils/apiResponseWriter";
import expressAsyncWrapper from "../utils/asyncHandler";
import { DMS_TABLES, ENTITIES_TABLES } from "../constants";
import { CleanResult, DataCleanupService } from "../services/azureOpenAi.service";
import zodErrorFmt from "../utils/zodErrorFmt";
import RouteError from "../utils/routeErrors";
import { cleanupBodyValidation } from "../validations/dataCleanup.validator";
import { FindManyModel } from "../types";
import { capitalizeWord } from "../utils";
import {
  PeopleRecord,
  UserMergeAIService,
  UserMergeInput,
  UserMergeOutput,
} from "../services/duplication-ai.service";
import z, { number } from "zod";
import { AIServiceResult } from "../services/aiService.service";
import { address, entity_property } from "../../generated/client/entities_prod";
import { writeFile } from "fs/promises";
import { mergeSimilarAddresses } from "../utils/merging";

export const cleanupTableDataController = expressAsyncWrapper(
  async (req, res) => {
    const service = new DataCleanupService();

    // Validate input
    const validationResult = cleanupBodyValidation.safeParse(req.body);
    if (!validationResult.success) {
      return APIResponseWriter({
        res,
        success: false,
        message: "Invalid request payload",
        statusCode: StatusCodes.BAD_REQUEST,
        error: zodErrorFmt(validationResult.error),
      });
    }

    const { db, table, keyField, page, limit, previewOnly } =
      validationResult.data;

    // Validate allowed tables
    const allowedTables =
      db === "dms" ? DMS_TABLES : db === "entities" ? ENTITIES_TABLES : [];

    if (!allowedTables.includes(table)) {
      return APIResponseWriter({
        res,
        success: false,
        message: `Table '${table}' is not allowed or does not exist in '${db}' database.`,
        statusCode: StatusCodes.BAD_REQUEST,
      });
    }

    // Get Prisma model
    const prismaClient = db === "dms" ? dmsPrisma : entitiesPrisma;

    const model = (prismaClient as unknown as Record<string, FindManyModel>)[
      table
    ];

    if (
      !model ||
      typeof model.findMany !== "function" ||
      typeof model.count !== "function"
    ) {
      return APIResponseWriter({
        res,
        success: false,
        message: `Table model '${table}' does not support findMany or count operations.`,
        statusCode: StatusCodes.BAD_REQUEST,
      });
    }

    // Calculate pagination
    const skip = (page - 1) * limit;

    // Fetch total count and paginated rows
    let total: number;
    let rows: Record<string, any>[];

    try {
      total = await model.count();
      rows = await model.findMany({
        skip,
        take: limit,
      });

      console.log(JSON.stringify(rows));
    } catch (error) {
      return APIResponseWriter({
        res,
        success: false,
        message: `Failed to fetch data from table '${table}'.`,
        statusCode: StatusCodes.INTERNAL_SERVER_ERROR,
      });
    }

    if (rows.length === 0) {
      return APIResponseWriter({
        res,
        success: true,
        message: `No data found in table '${table}' on page ${page}.`,
        data: {
          results: [],
        },
        statusCode: StatusCodes.OK,
      });
    }

    // Clean data using AI
    let results: CleanResult[];
    try {
      results = await service.cleanDataBatch(rows, keyField);
    } catch (error) {
      console.error("AI cleanup failed:", error);
      return APIResponseWriter({
        res,
        success: false,
        message: "Failed to process data cleanup. AI service may be down.",
        statusCode: StatusCodes.SERVICE_UNAVAILABLE,
      });
    }

    // Dry run: return preview
    if (previewOnly) {
      return APIResponseWriter({
        res,
        success: true,
        message: `Cleanup preview generated for '${table}' in '${db}'.`,
        statusCode: StatusCodes.OK,
        data: {
          count: results.length,
          needsReview: results.filter((r) => r.needsReview).length,
          suggestedUpdates: results.filter(
            (r) => !r.needsReview && Object.keys(r.changes).length > 0
          ).length,
          results,
        },
      });
    }

    // Apply changes to DB
    const response = await service.applyCleanup(db, table, results, keyField);

    return APIResponseWriter({
      res,
      success: true,
      message: `Cleanup completed for page ${page} of '${table}'.`,
      statusCode: StatusCodes.OK,
      data: {
        updatedCount: response.updatedCount,
        totalProcessed: results.length,
        needsReviewCount: results.filter((r) => r.needsReview).length,
        errors: response.errors,
      },
    });
  }
);

export const cleanupTableDataControllerTest = expressAsyncWrapper(
  async (req, res) => {
    const data = req.body;
    if (!Array.isArray(data)) {
      throw RouteError.BadRequest("Request body has to be array of objects");
    }

    const service = new DataCleanupService();
    // Clean data using AI
    let results: CleanResult[];
    try {
      results = await service.cleanDataBatch(data);
    } catch (error) {
      console.error("AI cleanup failed:", error);
      return APIResponseWriter({
        res,
        success: false,
        message: "Failed to process data cleanup. AI service may be down.",
        statusCode: StatusCodes.SERVICE_UNAVAILABLE,
      });
    }

    return APIResponseWriter({
      res,
      success: true,
      message: `Cleanup preview generated.`,
      statusCode: StatusCodes.OK,
      data: {
        count: results.length,
        needsReview: results.filter((r) => r.needsReview).length,
        suggestedUpdates: results.filter(
          (r) => !r.needsReview && Object.keys(r.changes).length > 0
        ).length,
        results,
      },
    });
  }
);

export const dmsUsersNameCapitalizerController = expressAsyncWrapper(
  async (req, res) => {
    const previewOnly = req.body.previewOnly ?? true;

    // Fetch only the needed fields
    const dmsUsers = await dmsPrisma.users.findMany({
      select: { id: true, first_name: true, last_name: true },
    });

    const updates = [];
    for (const u of dmsUsers) {
      const formattedFirst = capitalizeWord(u.first_name);
      const formattedLast = capitalizeWord(u.last_name || "");

      // Only push if changes are needed
      if (formattedFirst !== u.first_name || formattedLast !== u.last_name) {
        updates.push({
          id: u.id,
          first_name: formattedFirst,
          last_name: formattedLast,
        });
      }
    }

    if (!previewOnly && updates.length > 0) {
      // Batch update in parallel (chunk for large data sets)
      await Promise.all(
        updates.map((user) =>
          dmsPrisma.users.update({
            where: { id: user.id },
            data: {
              first_name: user.first_name,
              last_name: user.last_name,
            },
          })
        )
      );
    }

    return APIResponseWriter({
      res,
      message: previewOnly
        ? `${updates.length} users would be updated`
        : `${updates.length} users updated`,
      statusCode: 200,
      success: true,
      data: previewOnly ? updates : null,
    });
  }
);

export const dmsUsersPhoneNumberStandardizerController = expressAsyncWrapper(
  async () => {
    const phoneNumbers = await dmsPrisma.users.findMany({
      select: {
        id: true,
        phone_number: true,
        mobile_number: true,
        sms_number: true,
      },
    });

    // const addresses = await dmsPrisma.addresses.findMany({
    //   where: {
    //     entity_id: new BigInt(phoneNumbers[0].id)
    //   },
    //   select: {
    //     entity_id: true
    //   },

    // });
  }
);

export const contactsAndAddressTablesCleanupController = expressAsyncWrapper(
  async (_, res) => {
    const dmsAddresses = await dmsPrisma.addresses.findMany();
    const dmsLeadsTransactionsContacts =
      await dmsPrisma.leads_transactions_contacts.findMany();
    const entitiesContacts = await entitiesPrisma.entity_contact.findMany();
    const dmsGlobalEntityContacts =
      await dmsPrisma.global_entity_contacts.findMany();

    dmsPrisma.global_people.findFirst({
      select: {},
    });

    const entitiesAddresses = await entitiesPrisma.address.findMany();
    const dmsUsers = await dmsPrisma.users.findMany();

    res.send("DONE");

    // 1. formatting issues like FIRSTNAME LASTNAME fixed as Firstname Lastname and phone numbers from Pakistan for example, presented as 01234567890 changed to +92 123 456-7890 and so on;

    // 2. If there are three records for Saniul Islam where one record has your email, another your address and the third with your email, the system would merge them into 1 record;

    // 3. Mis-spellings fixed, e.g. Paikstan Pakistan.
  }
);

/**
 * Controller: getPotentialDuplicatePeopleGroupsController
 *
 * Finds people matching a name query, groups them by full name,
 * detects potential duplicates, and returns AI-powered merge recommendations.
 *
 * Only returns groups where ≥2 people share the same full name.
 *
 * Route: GET /api/people/duplicates/analyze
 * Query: ?name=John%20Doe
 */
export const getPotentialDuplicatePeopleGroupsController = expressAsyncWrapper(
  async (req, res) => {
    const rawName = (req.query.name as string) || "";

    if (!rawName?.trim()) {
      throw RouteError.BadRequest("Query parameter 'name' is required");
    }

    const nameParts = rawName.trim().split(/\s+/);
    const firstName = nameParts[0].toLowerCase();
    const lastName =
      nameParts.length > 1 ? nameParts.slice(1).join(" ").toLowerCase() : "";

    // Fetch matching people (case-insensitive)
    const people = await entitiesPrisma.people.findMany({
      where: {
        first_name: { in: [firstName, rawName] },
        ...(lastName && {
          last_name: { in: [lastName, ""] },
        }),
      },
      include: {
        entity: {
          include: {
            address: true,
            entity_property_entity_property_entity_idToentity: true,
          },
        },
      },
    });

    if (people.length === 0) {
      return APIResponseWriter({
        res,
        message: "No people found matching the given name",
        statusCode: StatusCodes.OK,
        success: true,
        data: { grouped: [], totalFound: 0 },
      });
    }

    await writeFile(`demo/${rawName}.json`, JSON.stringify(people));

    // === Step 1: Group by full name (case-insensitive) ===
    const fuseItems = people.map((p) => {
      const fn = p.first_name?.trim().toLowerCase() || "";
      const ln = p.last_name?.trim().toLowerCase() || "";
      const fullName = [fn, ln].filter(Boolean).join(" ");
      return { _fullName: fullName, _original: p };
    });

    const nameGroups = new Map<string, typeof fuseItems>();
    for (const item of fuseItems) {
      if (!item._fullName) continue;
      if (!nameGroups.has(item._fullName)) {
        nameGroups.set(item._fullName, []);
      }
      nameGroups.get(item._fullName)!.push(item);
    }

    // === Step 2: Only process groups with 2+ people ===
    const grouped: Array<{
      aiDecision: any;
      mergedPerson: any;
      deletionPlan: {
        retained_entity_id: number;
        retained_people_id: number;
        deleted_people_ids: number[];
        deleted_entity_ids: number[];
        tables_to_cleanup: Record<string, number[]>;
      };
    }> = [];

    const seen = new Set<number>();
    const aiService = new UserMergeAIService();

    for (const [fullName, group] of nameGroups) {
      // Skip if not a duplicate group
      if (group.length < 2) continue;

      for (const item of group) {
        const person = item._original;
        if (seen.has(person.people_id)) continue;

        const duplicates = group
          .map((i) => i._original)
          .filter(
            (p) => p.people_id !== person.people_id && !seen.has(p.people_id)
          );

        if (duplicates.length === 0) continue;

        // === AI Decision: Which to keep/remove ===
        const input: UserMergeInput = { primary: person, duplicates };
        const aiDecision = await aiService.call(input);

        const keptId = parseInt(aiDecision.keep);
        const removedIds = aiDecision.remove.map(Number);

        const keptRecord = [person, ...duplicates].find(
          (p) => p.people_id === keptId
        );
        const removedRecords = duplicates.filter((p) =>
          removedIds.includes(p.people_id)
        );

        if (!keptRecord) continue;

        // === Merge Properties (preserve all fields) ===
        const propMap = new Map<string, any>();
        const allProps = [
          ...keptRecord.entity
            .entity_property_entity_property_entity_idToentity,
          ...removedRecords.flatMap(
            (r) => r.entity.entity_property_entity_property_entity_idToentity
          ),
        ];

        const mergedAddresses = mergeSimilarAddresses(
          keptRecord.entity.address,
          removedRecords.flatMap((r) => r.entity.address)
        );

        for (const prop of allProps) {
          if (!prop.property_value) continue;
          const key = `${prop.property_id}_${prop.property_value
            .trim()
            .toLowerCase()}`;
          if (!propMap.has(key)) {
            propMap.set(key, {
              ...prop,
              entity_id: keptRecord.entity.entity_id,
            });
          } else {
            const existing = propMap.get(key);
            if (prop.is_primary === "Yes") {
              existing.is_primary = "Yes";
            }
          }
        }

        const mergedProperties = Array.from(propMap.values());

        const mergedPerson: PeopleRecord = {
          ...keptRecord,
          entity: {
            ...keptRecord.entity,
            address: mergedAddresses,
            entity_property_entity_property_entity_idToentity: mergedProperties,
          },
        };

        // === Deletion Plan ===
        const deleted_people_ids = removedRecords.map((r) => r.people_id);
        const deleted_entity_ids = removedRecords.map(
          (r) => r.entity.entity_id
        );

        const tables_to_cleanup: Record<string, number[]> = {
          people: deleted_people_ids,
          entity: deleted_entity_ids,
          entity_property: removedRecords.flatMap((r) =>
            r.entity.entity_property_entity_property_entity_idToentity.map(
              (p) => p.entity_property_id
            )
          ),
          address: deleted_entity_ids,
        };

        // Clean up empty entries
        Object.keys(tables_to_cleanup).forEach(
          (key) =>
            tables_to_cleanup[key].length === 0 && delete tables_to_cleanup[key]
        );

        grouped.push({
          aiDecision,
          mergedPerson,
          deletionPlan: {
            retained_entity_id: keptRecord.entity.entity_id,
            retained_people_id: keptRecord.people_id,
            deleted_people_ids,
            deleted_entity_ids,
            tables_to_cleanup,
          },
        });

        // Mark all in group as seen
        [...removedRecords, keptRecord].forEach((r) => seen.add(r.people_id));
      }
    }

    return APIResponseWriter({
      res,
      message: "Potential duplicate groups analyzed successfully",
      statusCode: StatusCodes.OK,
      success: true,
      data: {
        grouped,
        totalFound: people.length,
        duplicateGroupsCount: grouped.length,
      },
    });
  }
);

export const getPotentialDuplicatePeopleGroupsTestController =
  expressAsyncWrapper(async (req, res) => {
    const people = req.body as PeopleRecord[];

    // Enrich & group by full name
    const fuseItems = people.map((p) => {
      const firstName = p.first_name?.trim().toLowerCase() || "";
      const lastName = p.last_name?.trim().toLowerCase() || "";
      const fullName = [firstName, lastName].filter(Boolean).join(" ");
      return { _fullName: fullName, _original: p };
    });

    const nameGroups = new Map<string, typeof fuseItems>();
    for (const item of fuseItems) {
      if (!nameGroups.has(item._fullName)) nameGroups.set(item._fullName, []);
      nameGroups.get(item._fullName)!.push(item);
    }

    const grouped = [];
    const seen = new Set<number>();
    const aiService = new UserMergeAIService();

    for (const [_, group] of nameGroups) {
      for (const item of group) {
        const person = item._original;
        if (seen.has(person.people_id)) continue;

        const duplicates = group
          .map((i) => i._original)
          .filter(
            (p) => p.people_id !== person.people_id && !seen.has(p.people_id)
          );

        if (duplicates.length === 0) continue;

        const input: UserMergeInput = { primary: person, duplicates };
        const aiDecision = await aiService.call(input);

        const keptId = parseInt(aiDecision.keep);
        const removedIds = aiDecision.remove.map(Number);

        const keptRecord = [person, ...duplicates].find(
          (p) => p.people_id === keptId
        )!;

        const removedRecords = [person, ...duplicates].filter((p) =>
          removedIds.includes(p.people_id)
        );

        // === Merge properties with full field preservation ===
        const propMap = new Map<string, any>();

        const allProps = [
          ...keptRecord.entity
            .entity_property_entity_property_entity_idToentity,
          ...removedRecords.flatMap(
            (r) => r.entity.entity_property_entity_property_entity_idToentity
          ),
        ];

        const mergedAddresses = mergeSimilarAddresses(
          keptRecord.entity.address,
          removedRecords.flatMap((r) => r.entity.address)
        );

        for (const prop of allProps) {
          if (!prop.property_value) continue;
          const key = `${prop.property_id}_${prop.property_value
            .trim()
            .toLowerCase()}`;

          if (!propMap.has(key)) {
            propMap.set(key, {
              ...prop,
              entity_id: keptRecord.entity.entity_id,
            });
          } else {
            const existing = propMap.get(key);
            if (prop.is_primary === "Yes") {
              existing.is_primary = "Yes";
            }
          }
        }

        const mergedProperties = Array.from(propMap.values());

        const mergedPerson: PeopleRecord = {
          ...keptRecord,
          entity: {
            ...keptRecord.entity,
            address: mergedAddresses,
            entity_property_entity_property_entity_idToentity: mergedProperties,
          },
        };

        const deletionPlan = {
          retained_entity_id: keptRecord.entity.entity_id,
          retained_people_id: keptRecord.people_id,
          deleted_people_ids: removedRecords.map((r) => r.people_id),
          deleted_entity_ids: removedRecords.map((r) => r.entity.entity_id),
          tables_to_cleanup: {
            people: removedRecords.map((r) => r.people_id),
            entity: removedRecords.map((r) => r.entity.entity_id),
            entity_property: removedRecords.flatMap((r) =>
              r.entity.entity_property_entity_property_entity_idToentity.map(
                (p) => p.entity_property_id
              )
            ),
            address: removedRecords.map((r) => r.entity.entity_id),
          },
        };

        Object.keys(deletionPlan.tables_to_cleanup).forEach(
          (key) =>
            (deletionPlan.tables_to_cleanup as any)[key].length === 0 &&
            delete (deletionPlan.tables_to_cleanup as any)[key]
        );

        grouped.push({
          aiDecision,
          mergedPerson,
          deletionPlan,
        });

        removedRecords.forEach((r) => seen.add(r.people_id));
      }
    }

    res.json({ grouped, totalFound: people.length });
  });

export const getDuplicatePeopleByFullNameController = expressAsyncWrapper(
  async (req, res) => {
    // Fetch all people with minimal needed fields
    const people = await entitiesPrisma.people.findMany({
      where: {
        first_name: { not: null }, // exclude if first_name is null
      },
      select: {
        people_id: true,
        first_name: true,
        last_name: true,
      },
    });

    // Build full name and group
    const nameGroups = new Map<
      string,
      Array<{
        people_id: number;
        first_name: string | null;
        last_name: string | null;
      }>
    >();

    for (const person of people) {
      const firstName = person.first_name?.trim().toLowerCase() || "";
      const lastName = person.last_name?.trim().toLowerCase() || "";
      const fullName = [firstName, lastName].filter(Boolean).join(" ");

      // Skip if fullName is empty
      if (!fullName) continue;

      if (!nameGroups.has(fullName)) {
        nameGroups.set(fullName, []);
      }
      nameGroups.get(fullName)!.push(person);
    }

    // Filter: only keep groups with 2 or more people
    const duplicateGroups = [];
    for (const [fullName, group] of nameGroups) {
      if (group.length >= 2) {
        duplicateGroups.push(fullName);
      }
    }

    return APIResponseWriter({
      res,
      message: "Duplicate people grouped by full name retrieved successfully",
      statusCode: StatusCodes.OK,
      success: true,
      data: duplicateGroups,
    });
  }
);

export const getDuplicateEntitiesByFullNameController = expressAsyncWrapper(
  async (req, res) => {
    const entities = await entitiesPrisma.entity.findMany({
      where: {
        type: 2,
        // dups_ok: {
        //   not: -1,
        // },
        is_deleted: false,
      },
      select: {
        entity_id: true,
        name: true,
      },
    });

    // Build group
    const nameGroups = new Map<
      string,
      Array<{
        entity_id: number;
        name: string | null;
      }>
    >();

    for (const entity of entities) {
      const name = entity.name || "";

      // Skip if fullName is empty
      if (!name) continue;

      if (!nameGroups.has(name)) {
        nameGroups.set(name, []);
      }
      nameGroups.get(name)!.push(entity);
    }

    // Filter: only keep groups with 2 or more people
    const duplicateGroups = [];
    for (const [name, group] of nameGroups) {
      if (group.length >= 2) {
        duplicateGroups.push(name);
      }
    }

    return APIResponseWriter({
      res,
      message: "Duplicate entities grouped by name retrieved successfully",
      statusCode: StatusCodes.OK,
      success: true,
      data: duplicateGroups,
    });
  }
);

const paginationSchema = z.object({
  page: z.coerce.number().min(1).default(1),
  limit: z.coerce.number().min(1).default(20)
})

export const getDuplicateEntitiesByNameWithTypeController =
  expressAsyncWrapper(async (req, res) => {
    const type = req.params["type"];

    const { data: pagination, error: paginationError, success: isPaginationValid } = paginationSchema.safeParse(req.query)

    if (!isPaginationValid) {
      const [{ message }, ...rest] = zodErrorFmt(paginationError)
      throw RouteError.BadRequest(message, rest)
    }

    if (!type) {
      throw RouteError.BadRequest("Entity type not provided");
    }

    const parsedType = parseInt(type);
    const ENTITY_TYPES = [1, 2];

    if (!ENTITY_TYPES.find((i) => i === parsedType)) {
      throw RouteError.BadRequest("Invalid entity type provided. (" + parsedType + ")");
    }

    const { page, limit } = pagination

    const offset = (page - 1) * limit

    const entities = await entitiesPrisma.entity.findMany({
      where: {
        type: parsedType,
        // dups_ok: {
        //   not: -1,
        // },
        is_deleted: false,
        deleted_at: {
          equals: null
        }
      },
      select: {
        entity_id: true,
        name: true,
      },
    });

    // Build group
    const nameGroups = new Map<
      string,
      Array<{
        entity_id: number;
        name: string | null;
      }>
    >();

    for (const entity of entities) {
      const name = entity.name || "";

      // Skip if fullName is empty
      if (!name) continue;

      if (!nameGroups.has(name)) {
        nameGroups.set(name, []);
      }
      nameGroups.get(name)!.push(entity);
    }

    // Filter: only keep groups with 2 or more people
    const duplicateGroups: { name: string, duplicateCount: number }[] = [];
    for (const [name, group] of nameGroups) {
      if (group.length >= 2) {
        duplicateGroups.push({ name, duplicateCount: group.length });
      }
    }


    const paginatedResult = duplicateGroups.slice(offset, offset + limit)

    return APIResponseWriter({
      res,
      message: "Duplicate entities grouped by name retrieved successfully",
      statusCode: StatusCodes.OK,
      success: true,
      data: { duplicateGroups: paginatedResult, pagination: { limit, page, offset, total: duplicateGroups.length } },
    });
  });
