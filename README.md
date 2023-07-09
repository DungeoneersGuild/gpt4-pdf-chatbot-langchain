# GPT-4 & LangChain - Create a ChatGPT Chatbot for Your PDF Files

Use the new GPT-4 api to build a chatGPT chatbot for multiple Large PDF files.

Tech stack used includes LangChain, Pinecone, Typescript, Openai, and Next.js. LangChain is a framework that makes it easier to build scalable AI/LLM apps and chatbots. Pinecone is a vectorstore for storing embeddings and your PDF in text to later retrieve similar docs.

[Tutorial video](https://www.youtube.com/watch?v=ih9PBGVVOO4)

[Join the discord if you have questions](https://discord.gg/E4Mc77qwjm)

The visual guide of this repo and tutorial is in the `visual guide` folder.

**If you run into errors, please review the troubleshooting section further down this page.**

Prelude: Please make sure you have already downloaded node on your system and the version is 18 or greater.

## Development

1. Clone the repo or download the ZIP

```
git clone [github https url]
```

2. Install packages

First run `npm install yarn -g` to install yarn globally (if you haven't already).

Then run:

```
yarn install
```

After installation, you should now see a `node_modules` folder.

3. Set up your `.env` file

- Copy `.env.example` into `.env`
  Your `.env` file should look like this:

```
OPENAI_API_KEY=

PINECONE_API_KEY=
PINECONE_ENVIRONMENT=

PINECONE_INDEX_NAME=

```

- Visit [openai](https://help.openai.com/en/articles/4936850-where-do-i-find-my-secret-api-key) to retrieve API keys and insert into your `.env` file.
- Visit [pinecone](https://pinecone.io/) to create and retrieve your API keys, and also retrieve your environment and index name from the dashboard.

4. In `config/pinecone.ts`, replace the `PINECONE_INDEX_NAME` with the index name you
   created on Pinecone. Edit the `TOPICS` to match your desired topics, namespaces,
   and prompts. There should be a matching folder in `docs` for each of your topics. **THE FOLDER MUST EXACTLY MATCH THE TOPIC'S NAMESPACE AND MUST ONLY CONTAIN LOWER CASE LETTERS A-Z AND HYPHENS.**

5. In `utils/makechain.ts` chain adjust the `QA_PROMPT` according to your use case. Change `modelName` in `new OpenAIChat` to a different api model if you don't have access to `gpt-4`. See [the OpenAI docs](https://platform.openai.com/docs/models/model-endpoint-compatibility) for a list of supported `modelName`s. For example you could use `gpt-3.5-turbo` if you do not have access to `gpt-4`, yet.

## Convert your PDF files to embeddings

1. In `docs` add PDFs into the relevant folder for each topic. ChatGPT will show which PDF it referred to in the sources so give your PDFs descriptive names.

2. Run the script `pnpm run ingest` to 'ingest' and embed your docs

3. Check Pinecone dashboard to verify your namespace and vectors have been added.

## Run the app

Once you've verified that the embeddings and content have been successfully added to your Pinecone, you can run the app `npm run dev` to launch the local dev environment, and then type a question in the chat interface.

## Troubleshooting

In general, keep an eye out in the `issues` and `discussions` section of this repo for solutions.

**General errors**

- Make sure you're running the latest Node version. Run `node -v`
- Try a different PDF or convert your PDF to text first. It's possible your PDF is corrupted, scanned, or requires OCR to convert to text.
- `Console.log` the `env` variables and make sure they are exposed.
- Make sure you're using the same versions of LangChain and Pinecone as this repo.
- Check that you've created an `.env` file that contains your valid (and working) API keys, environment and index name.
- If you change `modelName` in `OpenAI`, make sure you have access to the api for the appropriate model.
- Make sure you have enough OpenAI credits and a valid card on your billings account.
- Check that you don't have multiple OPENAPI keys in your global environment. If you do, the local `env` file from the project will be overwritten by systems `env` variable.
- Try to hard code your API keys into the `process.env` variables if there are still issues.

**Pinecone errors**

- Make sure your pinecone dashboard `environment` and `index` matches the one in the `pinecone.ts` and `.env` files.
- Check that you've set the vector dimensions to `1536`.
- Switch your Environment in pinecone to `us-east1-gcp` if the other environment is causing issues.

**Deleting a namespace**

- If you need to clear a namespace and to re-ingest it again you can edit the target namespace and the
  target index in `scripts/delete-namespace.ts` and then perform `pnpm run delete-namespace`

If you're stuck after trying all these steps, delete `node_modules`, restart your computer, then `pnpm install` again.

## Credit

Frontend of this repo is inspired by [langchain-chat-nextjs](https://github.com/zahidkhawaja/langchain-chat-nextjs)
