FROM node:lts-alpine

RUN apk --update add postgresql-client

# Set the working directory
WORKDIR /app

# Copy only the package files initially to leverage Docker caching
RUN mkdir src && echo '' > src/index.ts
COPY ./package.json ./package-lock.json ./tsconfig.json ./

# Install dependencies for build
RUN npm ci

# Copy the rest of the application files
COPY . .

# Run the application
CMD ["npm", "run", "benchmark"]