import { StatusCodes } from "http-status-codes";
import { entitiesPrisma } from "../config/db";
import APIResponseWriter from "../utils/apiResponseWriter";
import expressAsyncWrapper from "../utils/asyncHandler";
import RouteError from "../utils/routeErrors";
import {
  address,
  entity_property,
  people,
} from "../../generated/client/entities_prod";
import { writeFile } from "fs/promises";

// === Types ===
interface ApplyMergePayload {
  keep_entity_id: number;
  remove_entity_ids: number[];
  merged_entity: {
    name?: string;
    trade_name?: string | null;
    people: Array<Omit<people, "entity_id">>;
    address: Array<Omit<address, "entity_id">>;
    entity_property_entity_property_entity_idToentity: Array<Omit<entity_property, "entity_id">>;
  };
}

const sanitizeDate = (date: any): Date | null => {
  if (!date || (typeof date === "object" && Object.keys(date).length === 0)) {
    return null;
  }
  try {
    const parsed = new Date(date);
    return isNaN(parsed.getTime()) ? null : parsed;
  } catch {
    return null;
  }
};

export const applyEntitiesDuplicateMergeController = expressAsyncWrapper(
  async (req, res) => {
    const payload: ApplyMergePayload = req.body;

    const {
      keep_entity_id,
      remove_entity_ids,
      merged_entity: {
        name,
        trade_name,
        people: mergedPeople,
        address: mergedAddresses,
        entity_property_entity_property_entity_idToentity: mergedProperties,
      },
    } = payload;

    // === Validation ===
    if (!keep_entity_id) {
      throw RouteError.BadRequest("Field 'keep_entity_id' is required");
    }

    if (!Array.isArray(remove_entity_ids) || remove_entity_ids.length === 0) {
      throw RouteError.BadRequest(
        "At least one entity must be in 'remove_entity_ids'"
      );
    }

    if (remove_entity_ids.includes(keep_entity_id)) {
      throw RouteError.BadRequest(
        "'keep_entity_id' cannot be in 'remove_entity_ids'"
      );
    }

    if (!Array.isArray(mergedPeople)) {
      throw RouteError.BadRequest(
        "At least one person is required in 'merged_entity.people'"
      );
    }

    if (!Array.isArray(mergedAddresses)) {
      throw RouteError.BadRequest("'merged_entity.address' must be an array");
    }

    if (!Array.isArray(mergedProperties)) {
      throw RouteError.BadRequest(
        "'merged_entity.properties' must be an array"
      );
    }

    const allEntityIds = [keep_entity_id, ...remove_entity_ids];

    // Fetch all involved entities in one query
    const entities = await entitiesPrisma.entity.findMany({
      where: {
        entity_id: { in: allEntityIds },
        is_deleted: false,
      },
      include: {
        people: true,
        address: true,
        entity_property_entity_property_entity_idToentity: true,
      },
    });

    if (entities.length !== allEntityIds.length) {
      const foundIds = entities.map((e) => e.entity_id);
      const missing = allEntityIds.filter((id) => !foundIds.includes(id));

      throw RouteError.BadRequest(
        `Invalid entity IDs: ${missing.join(", ")} not found or already deleted`
      );
    }

    const keptEntity = entities.find((e) => e.entity_id === keep_entity_id);

    if (!keptEntity) {
      throw RouteError.BadRequest("Keep entity not found");
    }

    const removedEntities = entities.filter((e) =>
      remove_entity_ids.includes(e.entity_id)
    );

    // === Prisma Transaction: Atomic Update & Delete ===
    try {
      await entitiesPrisma.$transaction(
        async (tx) => {
          const now = new Date();

          // 1. UPDATE: kept entity fields
          await tx.entity.update({
            where: { entity_id: keep_entity_id },
            data: {
              name: name ?? keptEntity.name,
              trade_name,
              updated_at: now,
            },
          });

          // 2. UPSERT: People (update existing or create new)
          for (const person of mergedPeople) {
            if (
              person.people_id &&
              keptEntity.people.some((p) => p.people_id === person.people_id)
            ) {
              await tx.people.update({
                where: { people_id: person.people_id },
                data: {
                  ...person,
                  entity_id: keep_entity_id,
                  date_of_birth: sanitizeDate(person.date_of_birth),
                  updated_at: now,
                  created_at: sanitizeDate(person.created_at),
                },
              });
            } else {
              const { people_id, ...dataWithoutId } = person;
              await tx.people.create({
                data: {
                  ...dataWithoutId,
                  entity_id: keep_entity_id,
                  date_of_birth: sanitizeDate(dataWithoutId.date_of_birth),
                  created_at: now,
                  updated_at: now,
                },
              });
            }
          }

          // Delete old people records from removed entities
          const peopleToDelete = removedEntities
            .flatMap((e) => e.people)
            .map((p) => p.people_id);

          if (peopleToDelete.length > 0) {
            await tx.people.updateMany({
              where: { people_id: { in: peopleToDelete } },
              data: { deleted_at: now },
            });
          }

          // 3. UPSERT: Addresses
          for (const addr of mergedAddresses) {
            if (addr.address_id) {
              const existing = keptEntity.address.find(
                (a) => a.address_id === addr.address_id
              );

              if (existing) {
                const updatedAddr = { is_primary: "", ...addr };
                const { address_id, is_primary, ...rest } = updatedAddr;
                await tx.address.update({
                  where: { address_id: address_id },
                  data: {
                    ...rest,
                    entity_id: keep_entity_id,
                    updated_at: now,
                    created_at: sanitizeDate(rest.created_at),
                  },
                });
              } else {
                const { address_id, ...dataWithoutId } = addr;
                await tx.address.create({
                  data: {
                    ...dataWithoutId,
                    entity_id: keep_entity_id,
                    created_at: now,
                    updated_at: now,
                  },
                });
              }
            } else {
              const { address_id, ...dataWithoutId } = addr;
              await tx.address.create({
                data: {
                  ...dataWithoutId,
                  entity_id: keep_entity_id,
                  created_at: now,
                  updated_at: now,
                },
              });
            }
          }

          // Delete old addresses
          const addressesToDelete = removedEntities
            .flatMap((e) => e.address)
            .map((a) => a.address_id);

          if (addressesToDelete.length > 0) {
            await tx.address.updateMany({
              where: { address_id: { in: addressesToDelete } },
              data: { deleted_at: now },
            });
          }

          // Small adjustment
          await tx.entity_property.deleteMany({
            where: {
              entity_id: keep_entity_id
            }
          })

          // 4. UPSERT: Properties
          for (const prop of mergedProperties) {
            if (prop.entity_property_id) {
              const existing =
                keptEntity.entity_property_entity_property_entity_idToentity.find(
                  (p) => p.entity_property_id === prop.entity_property_id
                );
              if (existing) {
                await tx.entity_property.update({
                  where: { entity_property_id: prop.entity_property_id },
                  data: {
                    ...prop,
                    entity_id: keep_entity_id,
                    updated_at: now,
                    created_at: sanitizeDate(prop.created_at),
                  },
                });
              } else {
                const { entity_property_id, ...dataWithoutId } = prop;
                await tx.entity_property.create({
                  data: {
                    ...dataWithoutId,
                    entity_id: keep_entity_id,
                    created_at: now,
                    updated_at: now,
                  },
                });
              }
            } else {
              const { entity_property_id, ...dataWithoutId } = prop;
              await tx.entity_property.create({
                data: {
                  ...dataWithoutId,
                  entity_id: keep_entity_id,
                  created_at: now,
                  updated_at: now,
                },
              });
            }
          }

          // Delete old properties
          const propertiesToDelete = removedEntities
            .flatMap((e) => e.entity_property_entity_property_entity_idToentity)
            .map((p) => p.entity_property_id);

          if (propertiesToDelete.length > 0) {
            await tx.entity_property.deleteMany({
              where: { entity_property_id: { in: propertiesToDelete } },
            });
          }

          // 5. SOFT DELETE: removed entities
          await tx.entity.updateMany({
            where: { entity_id: { in: remove_entity_ids } },
            data: {
              is_deleted: true,
              deleted_at: now,
            },
          });

          await writeFile(
            `logs/${keptEntity.name}-${Date.now().toLocaleString()}`,
            JSON.stringify({
              data: {
                action: "ENTITY_MERGE",
                entity_type: "entity",
                entity_id: keep_entity_id,
                old_value: `Merged from ${remove_entity_ids.length} duplicates`,
                new_value: `Merged entity updated with ${mergedPeople.length} people, ${mergedAddresses.length} addresses`,
                note: "Applied client-submitted merge plan",
                created_at: now,
              },
            })
          );
        },
        {
          timeout: 10_0000,
        }
      );

      return APIResponseWriter({
        res,
        message: "Duplicate merge applied successfully",
        statusCode: StatusCodes.OK,
        success: true,
        data: {
          merged_entity_id: keep_entity_id,
          deleted_entity_ids: remove_entity_ids,
          applied: true,
        },
      });
    } catch (error: any) {
      console.error("Merge Transaction Failed:", error);
      throw RouteError.BadRequest("Failed to apply merge: " + error.message);
    }
  }
);

export const applyEntitiesDuplicateMergeManuallyController = expressAsyncWrapper(
  async (req, res) => {
    const payload: ApplyMergePayload = req.body;

    const {
      keep_entity_id,
      remove_entity_ids,
      merged_entity: {
        name,
        trade_name,
        people: mergedPeople,
        address: mergedAddresses,
        entity_property_entity_property_entity_idToentity: mergedProperties,
      },
    } = payload;

    // === Validation ===
    if (!keep_entity_id) {
      throw RouteError.BadRequest("Field 'keep_entity_id' is required");
    }

    if (!Array.isArray(remove_entity_ids) || remove_entity_ids.length === 0) {
      throw RouteError.BadRequest(
        "At least one entity must be in 'remove_entity_ids'"
      );
    }

    if (remove_entity_ids.includes(keep_entity_id)) {
      throw RouteError.BadRequest(
        "'keep_entity_id' cannot be in 'remove_entity_ids'"
      );
    }

    if (!Array.isArray(mergedPeople)) {
      throw RouteError.BadRequest(
        "At least one person is required in 'merged_entity.people'"
      );
    }

    if (!Array.isArray(mergedAddresses)) {
      throw RouteError.BadRequest("'merged_entity.address' must be an array");
    }

    if (!Array.isArray(mergedProperties)) {
      throw RouteError.BadRequest(
        "'merged_entity.properties' must be an array"
      );
    }

    const allEntityIds = [keep_entity_id, ...remove_entity_ids];

    // Fetch all involved entities in one query
    const entities = await entitiesPrisma.entity.findMany({
      where: {
        entity_id: { in: allEntityIds },
        is_deleted: false,
      },
      include: {
        people: true,
        address: true,
        entity_property_entity_property_entity_idToentity: true,
      },
    });

    if (entities.length !== allEntityIds.length) {
      const foundIds = entities.map((e) => e.entity_id);
      const missing = allEntityIds.filter((id) => !foundIds.includes(id));

      throw RouteError.BadRequest(
        `Invalid entity IDs: ${missing.join(", ")} not found or already deleted`
      );
    }

    const keptEntity = entities.find((e) => e.entity_id === keep_entity_id);

    if (!keptEntity) {
      throw RouteError.BadRequest("Keep entity not found");
    }

    const removedEntities = entities.filter((e) =>
      remove_entity_ids.includes(e.entity_id)
    );

    // === Prisma Transaction: Atomic Update & Delete ===
    try {
      await entitiesPrisma.$transaction(
        async (tx) => {
          const now = new Date();

          // 1. UPDATE: kept entity fields
          await tx.entity.update({
            where: { entity_id: keep_entity_id },
            data: {
              name: name ?? keptEntity.name,
              trade_name,
              updated_at: now,
            },
          });

          // 2. UPSERT: People (update existing or create new)
          for (const person of mergedPeople) {
            if (
              person.people_id &&
              keptEntity.people.some((p) => p.people_id === person.people_id)
            ) {
              await tx.people.update({
                where: { people_id: person.people_id },
                data: {
                  ...person,
                  entity_id: keep_entity_id,
                  date_of_birth: sanitizeDate(person.date_of_birth),
                  updated_at: now,
                  created_at: sanitizeDate(person.created_at),
                },
              });
            } else {
              const { people_id, ...dataWithoutId } = person;
              await tx.people.create({
                data: {
                  ...dataWithoutId,
                  entity_id: keep_entity_id,
                  date_of_birth: sanitizeDate(dataWithoutId.date_of_birth),
                  created_at: now,
                  updated_at: now,
                },
              });
            }
          }

          // Delete old people records from removed entities
          const peopleToDelete = removedEntities
            .flatMap((e) => e.people)
            .map((p) => p.people_id);

          if (peopleToDelete.length > 0) {
            await tx.people.updateMany({
              where: { people_id: { in: peopleToDelete } },
              data: { deleted_at: now },
            });
          }

          // 3. UPSERT: Addresses
          for (const addr of mergedAddresses) {
            if (addr.address_id) {
              const existing = keptEntity.address.find(
                (a) => a.address_id === addr.address_id
              );

              if (existing) {
                const updatedAddr = { is_primary: "", ...addr };
                const { address_id, is_primary, ...rest } = updatedAddr;
                await tx.address.update({
                  where: { address_id: address_id },
                  data: {
                    ...rest,
                    entity_id: keep_entity_id,
                    updated_at: now,
                    created_at: sanitizeDate(rest.created_at),
                  },
                });
              } else {
                const { address_id, ...dataWithoutId } = addr;
                await tx.address.create({
                  data: {
                    ...dataWithoutId,
                    entity_id: keep_entity_id,
                    created_at: now,
                    updated_at: now,
                  },
                });
              }
            } else {
              const { address_id, ...dataWithoutId } = addr;
              await tx.address.create({
                data: {
                  ...dataWithoutId,
                  entity_id: keep_entity_id,
                  created_at: now,
                  updated_at: now,
                },
              });
            }
          }

          // Delete old addresses
          const addressesToDelete = removedEntities
            .flatMap((e) => e.address)
            .map((a) => a.address_id);

          if (addressesToDelete.length > 0) {
            await tx.address.updateMany({
              where: { address_id: { in: addressesToDelete } },
              data: { deleted_at: now },
            });
          }

          // Small adjustment
          await tx.entity_property.deleteMany({
            where: {
              entity_id: keep_entity_id
            }
          })

          // 4. UPSERT: Properties
          for (const prop of mergedProperties) {
            if (prop.entity_property_id) {
              const existing =
                keptEntity.entity_property_entity_property_entity_idToentity.find(
                  (p) => p.entity_property_id === prop.entity_property_id
                );
              if (existing) {
                await tx.entity_property.update({
                  where: { entity_property_id: prop.entity_property_id },
                  data: {
                    ...prop,
                    entity_id: keep_entity_id,
                    updated_at: now,
                    created_at: sanitizeDate(prop.created_at),
                  },
                });
              } else {
                const { entity_property_id, ...dataWithoutId } = prop;
                await tx.entity_property.create({
                  data: {
                    ...dataWithoutId,
                    entity_id: keep_entity_id,
                    created_at: now,
                    updated_at: now,
                  },
                });
              }
            } else {
              const { entity_property_id, ...dataWithoutId } = prop;
              await tx.entity_property.create({
                data: {
                  ...dataWithoutId,
                  entity_id: keep_entity_id,
                  created_at: now,
                  updated_at: now,
                },
              });
            }
          }

          // Delete old properties
          const propertiesToDelete = removedEntities
            .flatMap((e) => e.entity_property_entity_property_entity_idToentity)
            .map((p) => p.entity_property_id);

          if (propertiesToDelete.length > 0) {
            await tx.entity_property.deleteMany({
              where: { entity_property_id: { in: propertiesToDelete } },
            });
          }

          // 5. SOFT DELETE: removed entities
          await tx.entity.updateMany({
            where: { entity_id: { in: remove_entity_ids } },
            data: {
              is_deleted: true,
              deleted_at: now,
            },
          });

          await writeFile(
            `logs/${keptEntity.name}-${Date.now().toLocaleString()}`,
            JSON.stringify({
              data: {
                action: "ENTITY_MERGE",
                entity_type: "entity",
                entity_id: keep_entity_id,
                old_value: `Merged from ${remove_entity_ids.length} duplicates`,
                new_value: `Merged entity updated with ${mergedPeople.length} people, ${mergedAddresses.length} addresses`,
                note: "Applied client-submitted merge plan",
                created_at: now,
              },
            })
          );
        },
        {
          timeout: 10_0000,
        }
      );

      return APIResponseWriter({
        res,
        message: "Duplicate merge applied successfully",
        statusCode: StatusCodes.OK,
        success: true,
        data: {
          merged_entity_id: keep_entity_id,
          deleted_entity_ids: remove_entity_ids,
          applied: true,
        },
      });
    } catch (error: any) {
      console.error("Merge Transaction Failed:", error);
      throw RouteError.BadRequest("Failed to apply merge: " + error.message);
    }
  }
);
