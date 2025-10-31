import { PrismaClient as DMSPrismaClient } from "../../generated/client/dms_prod";
import { PrismaClient as EntitiesPrismaClient } from "../../generated/client/entities_prod";

const dmsPrisma = new DMSPrismaClient();
const entitiesPrisma = new EntitiesPrismaClient();

const convertBigIntToString = (obj: any): any => {
  if (typeof obj === "bigint") {
    return obj.toString();
  }
  if (Array.isArray(obj)) {
    return obj.map(convertBigIntToString);
  }
  if (typeof obj === "object" && obj !== null) {
    return Object.fromEntries(
      Object.entries(obj).map(([key, value]) => [
        key,
        convertBigIntToString(value),
      ])
    );
  }
  return obj;
};
//@ts-ignore
typeof dmsPrisma.$use === 'function' && dmsPrisma.$use(async (params, next) => {
  const result = await next(params);
  if (result === null || result === undefined) return result;

  return convertBigIntToString(result);
});
//@ts-ignore
typeof entitiesPrisma.$use === 'function' &&entitiesPrisma.$use(async (params, next) => {
  const result = await next(params);
  if (result === null || result === undefined) return result;

  return convertBigIntToString(result);
});

export { dmsPrisma, entitiesPrisma };
