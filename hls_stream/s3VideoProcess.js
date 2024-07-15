import dotenv from "dotenv";
import AWS from 'aws-sdk';
import fs from "fs"
import path  from "path";
import ffmpegStatic from "ffmpeg-static";
import ffmpeg from "fluent-ffmpeg";
import { resolve } from "dns";
ffmpeg.setFfmpegPath(ffmpegStatic)

dotenv.config();

const s3 = new AWS({
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretKeyId: process.env.AWS_SECRET_KEY_ID,
})


const mp4FileName = 'trial2.mp4';
const buketName = process.env.AWS_BUCKET;
const hlsFolder = 'hls';

const s3VideoProcess = async () => {
    console.log('Starting script');
    console.time('req_time');
    try {
        console.log('Doenloading s3 mp4 file locally');
        const mp4FilePath = `${mp4FileName}`;
        const writeStream = fs.createWriteStream('local.mp4');
        const readStreanm = s3.getObject({ Bucket: buketName, key : mp4FilePath }).createReadStream();
        readStreanm.pipe(writeStream);

        await new Promise((resolve, reject) => {
            writeStream.on('finish', resolve);
            writeStream.on('error', reject)
        });

        console.log('Downloaded s3 mp4 file locally');

        const resolutions = [
            {
                resolution: '320X180',
                videoBitrate: '500k',
                audioBitrate : '64k'
            },
            {
                resolution: '854X480',
                videoBitrate: '1000k',
                audioBitrate : '128k'
            },
            {
                resolution: '1280X780',
                videoBitrate: '2500k',
                audioBitrate : '192k'
            }
        ];

        const variantPlaylists = [];

        for (const { resolution, videoBitrate, audioBitrate } of resolutions) {
            console.log(`HLS conversion starting for ${resolution}`);
            const outputFileName = `${mp4FileName.replace(
                '.',
                '_'
            )}_${resolution}.m3u8`;
            const segmentFileName = `${mp4FileName.replace(
                '.',
                '_'
            )}_${resolution}_%03d.ts`;

            await new Promise((resolve,reject) => {
                ffmpeg('./local.mp4')
                   .outputOptions([
                      `-c:v h264`,
                      `b:v ${videoBitrate}`,
                      `-c:a aac`,
                      `-b:a ${audioBitrate}`,
                      `-vf scale=${resolution}`,
                      `-f hls`,
                      `-hls_time 10`,
                      `-hls_list_size 0`,
                      `-hls_segment_filename hls/${segmentFileName}`
                   ])
                    .output(`hls/${outputFileName}`)
                    .on('end', () => resolve())
                    .on('error', (err) => reject(err))
                    .run();
            });

            const variantPlaylist = {
                resolution,outputFileName
            };

            variantPlaylists.push(variantPlaylist);

            console.log(`HLS conversion done for ${resolution}`)
            
        }

        console.log(`HLS master m3u8 playlist generaing`);

        let masterPlaylist = variantPlaylists
                 .map((variantPlaylist) => {
                    const {resolution, outputFileName} = variantPlaylist;

                    const bandwidth = resolution === '320X180' ? 676800 : resolution === '854X480' ? 1353600 : 3230400;

                    return `#EXT-X-STREAM-INF:BANDWIDTH=${bandwidth},RESOLUTION=${resolution} \n ${outputFileName}`;
                 })
                 .join('\n');
                 masterPlaylist = `#EXTM3\n` + masterPlaylist

        const masterPlaylistFileName = `${mp4FileName.replace(
            '.',
            '_'
        )}_master.m3u8`;

        const masterPlaylistPath = `hls/${masterPlaylistFileName}`;
        fs.writeFileSync(masterPlaylistPath, masterPlaylist)

        console.log(`HLS master m3u8 playlist generated`);

        console.log(`Deleting locally downloaded s3 mp4 file`)

        fs.unlinkSync('local.mp4');

        console.log(`Uploading media m3u8 playlists and ts segment to s3`)

        const files = fs.readdirSync(hlsFolder);

        for(const file of files) {
            if(!file.startsWith(mp4FileName.replace('.','_'))){
                continue;
            }

            const filePath = path.join(hlsFolder,file);
            const fileStream = fs.createReadStream(filePath);

            const uploadParams = {
                Bucket: bucketName,
                Key : `${hlsFolder}/${file}`,
                Body: fileStream,
                ContentType : file.endsWith('.ts')
                              ? 'vidoe/mp2t' : file.endsWith('m3u8') ? 'application/x-mpegURL' : null
            };

            await s3.upload(uploadParams).promise();
            fs.unlink(filePath);
        }

        console.log('Success. Time taken =')
        console.time('req_time');
    } catch (error) {
        console.log('error' , error);
    }
}

export default s3VideoProcess;