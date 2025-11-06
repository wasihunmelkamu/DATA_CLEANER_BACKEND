import { PrismaClient as DMSPrismaClient } from '../generated/client/dms_prod/index.js';
import { PrismaClient as EntitiesPrismaClient } from '../generated/client/entities_prod/index.js';

const dms = new DMSPrismaClient();
const entities = new EntitiesPrismaClient();

async function logTickets() {
  try {
    let tickets = [];

    // Try fetching from DMS database
    try {
      const ticket = await dms.leads_tickets.findFirst();
      tickets.push(ticket)
      if (tickets.length > 0) {
        console.log("✅ Tickets found in DMS database:");
        console.table(tickets);
        return; // stop after finding
      }
    } catch (err) {
      console.log("⚠️ No 'ticket' table in DMS database or query failed.");
    }

    // Try fetching from Entities database
    try {
     // tickets = await entities.findMany();
      if (tickets.length > 0) {
        console.log("✅ Tickets found in Entities database:");
       // console.log(tickets);
        return;
      }
    } catch (err) {
      console.log("⚠️ No 'ticket' table in Entities database or query failed.");
    }

    console.log("❌ No 'ticket' table found in either database.");
  } catch (error) {
    console.error("Unexpected error:", error);
  } finally {
    await dms.$disconnect();
    await entities.$disconnect();
  }
}

logTickets();



