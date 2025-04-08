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

// MQTT client setup
console.log("Connecting to MQTT broker...");
const client = mqtt.connect(
  process.env.MQTT_BROKER_URL || "mqtt://test.mosquitto.org:1883"
);

// Connection event handler
client.on("connect", () => {
  console.log("Successfully connected to MQTT broker");

  // Subscribe to the topic your M5Stack is publishing to
  client.subscribe("temperature");

  console.log(
    'Server is now running and listening for MQTT messages on topic "temperature"...'
  );
});

// Message event handler
client.on("message", async (topic, message) => {
  const messageStr = message.toString();
  console.log(
    `[${new Date().toISOString()}] Received message on topic ${topic}: ${messageStr}`
  );

  try {
    // Parse the JSON data
    const data = JSON.parse(messageStr);

    // Add timestamp to the data
    const dataWithTimestamp = {
      ...data,
      timestamp: serverTimestamp(),
      receivedAt: new Date().toISOString(), // For easier querying and display
    };

    // Save to Firestore
    const docRef = await addDoc(
      collection(db, "sensorData"),
      dataWithTimestamp
    );
    console.log(`Data saved to Firestore with ID: ${docRef.id}`);

    // Optional: Add some basic data validation or alerts
    if (data.temperature > 30) {
      console.log(`⚠️ HIGH TEMPERATURE ALERT: ${data.temperature}°C`);
    }
  } catch (error) {
    console.error("Error processing message:", error);
  }
});

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
