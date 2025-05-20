# Dockerfile
FROM node:22

WORKDIR /usr/src/app

# Copy package manifests
COPY package.json package-lock.json* ./

# Install prod deps (falls back to npm install if no lockfile)
RUN npm install --only=production

# Copy rest of your code
COPY . .

# Run your job
CMD ["node", "index.js"]
