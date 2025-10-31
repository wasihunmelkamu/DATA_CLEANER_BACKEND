// Use the generated clients for DMS and Entities
const { PrismaClient: DMSPrismaClient } = require('./generated/client/dms_prod');
const { PrismaClient: EntitiesPrismaClient } = require('./generated/client/entities_prod');

const dms = new DMSPrismaClient();
const entities = new EntitiesPrismaClient();

const searchTerm = process.argv[2];
if (!searchTerm) {
  console.error('❌ Please provide a search term.');
  console.log('👉 Example: node searcher.js "name of five  indian people"');
  process.exit(1);
}

// Helper to list model names that support findMany
function listQueryableModels(prisma) {
  return Object.keys(prisma).filter(
    (k) => prisma[k] && typeof prisma[k].findMany === 'function'
  );
}

async function searchDb(dbName, prisma) {
  const modelNames = listQueryableModels(prisma);
  console.log(`\n🔍 Searching "${searchTerm}" in ${dbName} across ${modelNames.length} models...\n`);

  for (const modelName of modelNames) {
    try {
      const one = await prisma[modelName].findFirst();
      if (!one) continue;

      const stringFields = Object.keys(one).filter((k) => typeof one[k] === 'string');
      if (stringFields.length === 0) continue;

      const whereClause = {
        OR: stringFields.map((field) => ({ [field]: { contains: searchTerm } })),
      };

      const results = await prisma[modelName].findMany({ where: whereClause, take: 5 });
      if (results.length > 0) {
        console.log(`📂 Found in ${dbName}.${modelName}`);
        console.dir(results, { depth: null });
        console.log('----------------------\n');
      }
    } catch (_) {
      // skip non-queryable or restricted models
    }
  }
}

async function main() {
  try {
    await searchDb('DMS', dms);
    await searchDb('Entities', entities);
  } finally {
    await dms.$disconnect();
    await entities.$disconnect();
    console.log('✅ Search complete.');
  }
}

main();