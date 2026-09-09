/*
 * NiFi ExecuteScript (Groovy) processor: produces ONE credit-card transaction
 * per FlowFile, in exactly the JSON contract of generator/generator.py
 * (see jobs/common/schema.py - the Spark job rejects anything else).
 *
 * Why a script instead of GenerateFlowFile?  GenerateFlowFile can only emit
 * static text; we need per-record randomness + a valid ISO timestamp.
 *
 * In the UI: ExecuteScript -> Script Engine: groovy -> put this in "Script Body",
 * then connect  GenerateFlowFile(count=1, every 1s) -> ExecuteScript -> PublishKafka.
 * Set Kafka bootstrap.servers = kafka:29092, topic.name = raw_transactions.
 */
flowFiles = session.create(1)
ff = flowFiles[0]

def rnd = new Random()
def merchants = (0..119).collect { String.format("MER-%04d", it) }
def cards     = (0..199).collect { String.format("CARD-%05d", it) }
def countries = ["US","GB","DE","FR","ES","IT","NL","PL","CA","BR","MX","IN","SG","JP","AU","AE","TR"]
def channels  = ["online","pos","atm"]
def cats      = ["grocery","fuel","restaurant","retail","pharmacy","travel","electronics",
                 "gambling","crypto","wire_transfer","digital_goods","atm"]

// ---- the same "fraud is a pattern, not a coin flip" idea as generator/simulator.py
boolean fraud = rnd.nextDouble() < 0.05
double amount
String channel = channels[rnd.nextInt(channels.size())]
String country = countries[rnd.nextInt(countries.size())]
if (fraud) {
    int pick = rnd.nextInt(4)
    if (pick == 0)      { amount = 3000 + rnd.nextDouble() * 12000 }          // big ticket
    else if (pick == 1) { channel = "online"; country = "BR"; amount = 5 + rnd.nextDouble() * 3 } // card testing
    else if (pick == 2) { channel = "atm";    amount = 800 + rnd.nextDouble() * 1500 }             // cash out
    else                { channel = "online"; country = "NG"; amount = 500 + rnd.nextDouble() * 3000 }
} else {
    amount = Math.max(1.0, 60 + rnd.nextGaussian() * 55)
}

long now = System.currentTimeMillis()
String ts = java.time.Instant.ofEpochMilli(now).toString().replace(".000", "").replaceAll("\\.\\d+", "")

String json = String.format(
  '{"transaction_id":"TXN-%d-%08x","event_ts":"%s","card_id":"%s","merchant_id":"%s",' +
  '"amount":%.2f,"currency":"USD","channel":"%s","merchant_country":"%s","card_present":%s,' +
  '"merchant_category":"%s","is_3ds":%s,"device_id":"DEV-%06x"}',
  now, rnd.nextInt(0xFFFFFFFF as int), ts,
  cards[rnd.nextInt(cards.size())],
  merchants[rnd.nextInt(merchants.size())],
  amount, channel, country,
  (channel == "pos" || channel == "atm") ? "true" : "false",
  cats[rnd.nextInt(cats.size())],
  channel == "online" ? (rnd.nextDouble() < 0.6 ? "true" : "false") : "false",
  rnd.nextInt(0xFFFFFF))

ff.write(json.getBytes("UTF-8"))
ff.putAttribute("kafka.key", "kafka_message_key")            // PublishKafka key attribute
ff.putAttribute("mime.type", "application/json")
ff.putAttribute("event_ts", ts)
session.transfer(ff, REL_SUCCESS)
