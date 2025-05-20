# Use the official Node 22 image as the base
FROM node:22

# Create and set the working directory
WORKDIR /usr/src/app

# Copy package manifests and install only production deps
COPY package*.json ./
RUN npm ci --only=production

# Copy all application source code
COPY . .

# Run your entrypoint script
CMD ["node", "index.js"]
