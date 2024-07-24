import express from "express";
import cors from "cors";
import multer from "multer";
import { v4 as uuidv4 } from "uuid";
import path from "path";
import fs from "fs";
import { exec } from "child_process";
import { Kafka } from "kafkajs";

const app = express();

// Initialize Kafka
const kafka = new Kafka({
  clientId: "video-app",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "video-group" });

// Multer middleware
const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    cb(null, "./uploads");
  },
  filename: function (req, file, cb) {
    cb(null, file.fieldname + "-" + uuidv4() + path.extname(file.originalname));
  },
});

// Multer configuration
const upload = multer({ storage: storage });

app.use(
  cors({
    origin: ["http://localhost:3000", "http://localhost:5173"],
    credentials: true,
  })
);

app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header(
    "Access-Control-Allow-Headers",
    "Origin, X-Requested-With, Content-Type, Accept"
  );
  next();
});

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use("/uploads", express.static("uploads"));

app.get("/", function (req, res) {
  res.json({ message: "Hello chai aur code" });
});

app.post("/upload", upload.single("file"), async function (req, res) {
  const lessonId = uuidv4();
  const videoPath = req.file.path;

  // Produce a message to Kafka
  await producer.connect();
  await producer.send({
    topic: "video-uploads",
    messages: [
      {
        key: lessonId,
        value: JSON.stringify({ lessonId, videoPath }),
      },
    ],
  });

  res.json({
    message: "Video uploaded successfully, processing started",
    lessonId: lessonId,
  });
});

// Consumer to process video
const runConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: "video-uploads", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const { lessonId, videoPath } = JSON.parse(message.value.toString());
      const outputPath = `./uploads/courses/${lessonId}`;
      const hlsPath = `${outputPath}/index.m3u8`;

      if (!fs.existsSync(outputPath)) {
        fs.mkdirSync(outputPath, { recursive: true });
      }

      // ffmpeg
      const ffmpegCommand = `ffmpeg -i ${videoPath} -codec:v libx264 -codec:a aac -hls_time 10 -hls_playlist_type vod -hls_segment_filename "${outputPath}/segment%03d.ts" -start_number 0 ${hlsPath}`;

      exec(ffmpegCommand, (error, stdout, stderr) => {
        if (error) {
          console.error(`exec error: ${error}`);
          return;
        }
        console.log(`stdout: ${stdout}`);
        console.log(`stderr: ${stderr}`);
        const videoUrl = `http://localhost:8080/uploads/courses/${lessonId}/index.m3u8`;

        // You can send the videoUrl to another topic if needed
        // producer.send({ topic: 'processed-videos', messages: [{ key: lessonId, value: videoUrl }] });

        console.log({
          message: "Video converted to HLS format",
          videoUrl: videoUrl,
          lessonId: lessonId,
        });
      });
    },
  });
};

runConsumer().catch(console.error);

app.listen(8080, function () {
  console.log("App is listening at port 8080...");
});
