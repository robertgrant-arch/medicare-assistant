FROM node:20-slim

RUN apt-get update && apt-get install -y unzip curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package.json ./
RUN npm install --production

COPY . .

EXPOSE 3000

# Default: run the web server
CMD ["node", "src/server.js"]
