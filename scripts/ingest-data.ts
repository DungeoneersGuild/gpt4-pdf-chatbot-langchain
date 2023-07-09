import fs from 'fs';
import path from 'path';
import { RecursiveCharacterTextSplitter } from 'langchain/text_splitter';
import { OpenAIEmbeddings } from 'langchain/embeddings';
import { PineconeStore } from 'langchain/vectorstores';
import { pinecone } from '@/utils/pinecone-client';
import { PDFLoader } from 'langchain/document_loaders';
import { PINECONE_INDEX_NAME } from '@/config/pinecone';
import { hashElement } from 'folder-hash';

function getPreviousHash(fulldir: string): Object {
  try {
    const dirname = path.basename(fulldir);
    const data = fs.readFileSync(`./history/${dirname}.json`, 'utf8');
    const parsed = JSON.parse(data);
    console.log('parsed', typeof parsed, parsed);
    return parsed;
  } catch (err) {
    console.error(err);
    return { hash: '' };
  }
}

function setPreviousHash(hash: Object, fulldir: string) {
  try {
    const data = JSON.stringify(hash);
    const dirname = path.basename(fulldir);
    fs.writeFileSync(`./history/${dirname}.json`, data);
  } catch (err) {
    console.error(err);
  }
}

function recordFinishedLocally(directory: string) {
  const options = {
    folders: { exclude: ['.*', 'node_modules', 'test_coverage'] },
    files: { include: ['*.pdf'] },
  };
  hashElement(directory, options)
    .then((hash: any) => {
      setPreviousHash(hash, directory);
    })
    .catch((error: any) => {
      return console.error('hashing failed:', error);
    });
}

function checkDiff(directory: string, callback: Function) {
  const prevHash = getPreviousHash(directory);
  const options = {
    folders: { exclude: ['.*', 'node_modules', 'test_coverage'] },
    files: { include: ['*.pdf'] },
  };
  hashElement(directory, options)
    .then((hash: any) => {
      const newHash = hash;
      if (newHash.hash === prevHash.hash) {
        console.log('no changes detected');
        return false;
      } else {
        console.log('changes detected');
        console.log(
          'newHash',
          newHash.hash,
          'prevHash',
          prevHash.hash,
          typeof prevHash,
        );
        return callback();
      }
    })
    .catch((error: any) => {
      return console.error('hashing failed:', error);
    });
}

export const run = async () => {
  try {
    /* Load all directories */
    const directories = fs
      .readdirSync('./docs')
      .filter((file) => {
        return fs.statSync(path.join('./docs', file)).isDirectory();
      })
      .map((dir) => `./docs/${dir}`); // Add prefix 'docs/' to directory names
    console.log('directories: ', directories);
    for (const directory of directories) {
      /* Load all PDF files in the directory */
      checkDiff(directory, async () => {
        const files = fs
          .readdirSync(directory)
          .filter((file) => path.extname(file) === '.pdf');

        for (const file of files) {
          console.log(`Processing file: ${file}`);

          /* Load raw docs from the pdf file */
          const filePath = path.join(directory, file);
          const loader = new PDFLoader(filePath);
          const rawDocs = await loader.load();

          // console.log(rawDocs);

          /* Split text into chunks */
          const textSplitter = new RecursiveCharacterTextSplitter({
            chunkSize: 1000,
            chunkOverlap: 200,
          });

          const docs = await textSplitter.splitDocuments(rawDocs);
          // console.log('split docs', docs);

          // console.log('creating vector store...');
          /*create and store the embeddings in the vectorStore*/
          const embeddings = new OpenAIEmbeddings();
          const index = pinecone.Index(PINECONE_INDEX_NAME);
          const namespace = path.basename(directory); // use the directory name as the namespace
          // console.log("creating vector store with namespace: ", namespace)
          //embed the PDF documents

          /* Pinecone recommends a limit of 100 vectors per upsert request to avoid errors*/
          const chunkSize = 50;
          for (let i = 0; i < docs.length; i += chunkSize) {
            const chunk = docs.slice(i, i + chunkSize);
            // await PineconeStore.fromDocuments(
            //   index,
            //   chunk,
            //   embeddings,
            //   'text',
            //   namespace,
            // );
          }

          console.log(`File ${file} processed`);
          recordFinishedLocally(directory);
        }
      });
    }
  } catch (error) {
    console.log('error', error);
    throw new Error('Failed to ingest your data');
  }
};

(async () => {
  await run();
  console.log('completed ingestion of all PDF files in all directories');
})();
