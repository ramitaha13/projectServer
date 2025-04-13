const mqtt = require("mqtt");
const { initializeApp } = require("firebase/app");
const {
  getFirestore,
  collection,
  addDoc,
  serverTimestamp,
} = require("firebase/firestore");
require("dotenv").config();

// Firebase configuration
const firebaseConfig = {
  apiKey: process.env.FIREBASE_API_KEY,
  authDomain: process.env.FIREBASE_AUTH_DOMAIN,
  projectId: process.env.FIREBASE_PROJECT_ID,
  storageBucket: process.env.FIREBASE_STORAGE_BUCKET,
  messagingSenderId: process.env.FIREBASE_MESSAGING_SENDER_ID,
  appId: process.env.FIREBASE_APP_ID,
};

// Initialize Firebase
const firebaseApp = initializeApp(firebaseConfig);
const db = getFirestore(firebaseApp);

// Store the latest data
let latestData = null;
let lastSaveTime = 0;

// MQTT client setup
console.log("Connecting to MQTT broker...");
const client = mqtt.connect(
  process.env.MQTT_BROKER_URL || "mqtt://test.mosquitto.org:1883"
);

// Connection event handler
client.on("connect", () => {
  console.log("Successfully connected to MQTT broker");

  // Subscribe to the topic your M5Stack is publishing to
  client.subscribe("Braude/project/R&K");

  console.log(
    'Server is now running and listening for MQTT messages on topic "Braude/project/R&K"...'
  );
});

// Function to validate data with safety checks
function isValidData(data) {
  // First check if data is an object
  if (!data || typeof data !== "object") {
    console.log(
      `Error: Received data is not an object. Type: ${typeof data}, Value: ${data}`
    );
    return false;
  }

  // Check if both temperature and Light properties exist
  const hasTemperature =
    "temperature" in data &&
    data.temperature !== null &&
    data.temperature !== undefined;
  const hasLight =
    "Light" in data && data.Light !== null && data.Light !== undefined;

  if (!hasTemperature) {
    console.log("Error: Temperature data missing or invalid");
  }

  if (!hasLight) {
    console.log("Error: Light data missing or invalid");
  }

  return hasTemperature && hasLight;
}

// Function to save data to Firestore
async function saveToFirestore(data) {
  try {
    // Validate data first
    if (!isValidData(data)) {
      console.log(
        `⚠️ Invalid data. Missing temperature or Light values. Skipping Firestore save.`
      );
      return;
    }

    // Skip saving if temperature is over 50
    if (data.temperature > 50) {
      console.log(
        `⚠️ Temperature too high (${data.temperature}°C). Skipping Firestore save.`
      );
      return;
    }

    // Add timestamp to the data
    const dataWithTimestamp = {
      ...data,
      timestamp: serverTimestamp(),
      receivedAt: new Date().toISOString(),
    };

    // Save to Firestore
    const docRef = await addDoc(
      collection(db, "sensorData"),
      dataWithTimestamp
    );
    console.log(`✅ Data saved to Firestore with ID: ${docRef.id}`);
    console.log(`Temperature: ${data.temperature}°C, Light: ${data.Light} lux`);

    // Add humidity logging if it exists
    if ("humidity" in data) {
      console.log(`Humidity: ${data.humidity}`);
    }

    lastSaveTime = Date.now();

    // Alert for high temperature (but below 50)
    if (data.temperature > 30) {
      console.log(`⚠️ HIGH TEMPERATURE ALERT: ${data.temperature}°C`);
    }
  } catch (error) {
    console.error("Error saving to Firestore:", error);
  }
}

// Message event handler
client.on("message", async (topic, message) => {
  const messageStr = message.toString();
  console.log(
    `[${new Date().toISOString()}] Received message on topic ${topic}: ${messageStr}`
  );

  try {
    // Parse the JSON data
    const data = JSON.parse(messageStr);

    // Debug: Log the type and keys of data
    console.log(`Data type: ${typeof data}`);
    if (typeof data === "object" && data !== null) {
      console.log(`Data keys: ${Object.keys(data)}`);
      latestData = data;

      // Check if it's time to save (60 seconds since last save)
      const now = Date.now();
      if (now - lastSaveTime >= 60000) {
        await saveToFirestore(data);
      } else {
        console.log(
          `Received data but waiting to save. Next save in ${Math.round(
            (60000 - (now - lastSaveTime)) / 1000
          )} seconds.`
        );
      }
    } else {
      console.log(`Warning: Received non-object data: ${data}`);
    }
  } catch (error) {
    console.error("Error processing message:", error);
  }
});

// Set up interval to check and save data every 60 seconds
setInterval(async () => {
  const now = Date.now();
  if (latestData && now - lastSaveTime >= 60000) {
    console.log("1 minute elapsed, saving data to Firestore...");
    try {
      await saveToFirestore(latestData);
    } catch (error) {
      console.error("Error in scheduled save:", error);
    }
  }
}, 1000); // Check every second, but only save if 60 seconds have passed

// Error event handler
client.on("error", (err) => {
  console.error("MQTT error:", err);
});

// Reconnect event handler
client.on("reconnect", () => {
  console.log("Attempting to reconnect to MQTT broker...");
});

// Offline event handler
client.on("offline", () => {
  console.log("MQTT client is offline. Waiting to reconnect...");
});

// Handle graceful shutdown
process.on("SIGINT", () => {
  console.log("Closing MQTT connection and exiting...");
  client.end();
  process.exit();
});

// Keep the process running
process.stdin.resume();

console.log("M5Stack MQTT to Firebase server started.");
