import express from "express"
import cors from "cors"
import dotenv from "dotenv";
import KafkaConfig from "./apache-kafka/kafka";
import convertToHLS from "./hls_stream/transcode_video";
import s3VideoProcess from "./hls_stream/s3VideoProcess";

dotenv.config();

const port = 9001;

const app = express();

app.use(cors({
    allowedHeaders: ["*"],
    origin: "*"
}));

app.use(express.json());

app.get('/transcode', (req,res) => {
    s3VideoProcess();
    convertToHLS();

    res.send('transcoding done.')
})

const kafkaconfig = new KafkaConfig();
kafkaconfig.consume("transcode", (value) => {
    console.log("Got data from kafka : " , value)
})

app.listen(port, () => {
    console.log(`Server is running at http:localhost:${port}`);
})