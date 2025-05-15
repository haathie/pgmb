FROM node:lts-alpine

# Set the working directory
WORKDIR /app

# Copy only the package files initially to leverage Docker caching
COPY ./package.json ./package-lock.json ./

# Install dependencies for build
RUN npm ci

# Copy the rest of the application files
COPY . .

# Run the application
CMD ["npm", "run", "benchmark:tsc"]