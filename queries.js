const { MongoClient } = require('mongodb');

class BookQueries {
    constructor() {
        this.uri = "mongodb://127.0.0.1:27017/library";
        this.client = new MongoClient(this.uri);
        this.database = null;
        this.books = null;
    }

    async connect() {
        try {
            await this.client.connect();
            console.log("✅ Connected to MongoDB successfully\n");
            this.database = this.client.db('library');
            this.books = this.database.collection('books');
            return true;
        } catch (error) {
            console.error('❌ Connection failed:', error.message);
            return false;
        }
    }

    async disconnect() {
        await this.client.close();
        console.log("Connection closed");
    }

    // TASK 2: Basic CRUD Operations
    async initializeSampleData() {
        console.log("📚 TASK 2: BASIC CRUD OPERATIONS");
        console.log("=".repeat(50));

        const sampleBooks = [
            {
                title: "The Great Gatsby",
                author: "F. Scott Fitzgerald",
                genre: "Classic",
                published_year: 1925,
                price: 10.99,
                in_stock: true,
                pages: 180,
                publisher: "Scribner"
            },
            {
                title: "To Kill a Mockingbird",
                author: "Harper Lee",
                genre: "Fiction",
                published_year: 1960,
                price: 12.50,
                in_stock: true,
                pages: 281,
                publisher: "J.B. Lippincott & Co."
            },
            {
                title: "1984",
                author: "George Orwell",
                genre: "Dystopian",
                published_year: 1949,
                price: 9.99,
                in_stock: true,
                pages: 328,
                publisher: "Secker & Warburg"
            },
            {
                title: "Pride and Prejudice",
                author: "Jane Austen",
                genre: "Romance",
                published_year: 1813,
                price: 8.99,
                in_stock: false,
                pages: 432,
                publisher: "T. Egerton"
            },
            {
                title: "The Hobbit",
                author: "J.R.R. Tolkien",
                genre: "Fantasy",
                published_year: 1937,
                price: 11.99,
                in_stock: true,
                pages: 310,
                publisher: "George Allen & Unwin"
            },
            {
                title: "The Catcher in the Rye",
                author: "J.D. Salinger",
                genre: "Fiction",
                published_year: 1951,
                price: 10.49,
                in_stock: true,
                pages: 234,
                publisher: "Little, Brown and Company"
            },
            {
                title: "The Lord of the Rings",
                author: "J.R.R. Tolkien",
                genre: "Fantasy",
                published_year: 1954,
                price: 24.99,
                in_stock: true,
                pages: 1178,
                publisher: "George Allen & Unwin"
            },
            {
                title: "Harry Potter and the Philosopher's Stone",
                author: "J.K. Rowling",
                genre: "Fantasy",
                published_year: 1997,
                price: 14.99,
                in_stock: true,
                pages: 223,
                publisher: "Bloomsbury"
            },
            {
                title: "The Da Vinci Code",
                author: "Dan Brown",
                genre: "Mystery",
                published_year: 2003,
                price: 13.99,
                in_stock: false,
                pages: 454,
                publisher: "Doubleday"
            },
            {
                title: "The Alchemist",
                author: "Paulo Coelho",
                genre: "Fiction",
                published_year: 1988,
                price: 11.25,
                in_stock: true,
                pages: 208,
                publisher: "HarperTorch"
            },
            {
                title: "The Martian",
                author: "Andy Weir",
                genre: "Science Fiction",
                published_year: 2014,
                price: 14.99,
                in_stock: true,
                pages: 369,
                publisher: "Crown"
            },
            {
                title: "Project Hail Mary",
                author: "Andy Weir",
                genre: "Science Fiction",
                published_year: 2021,
                price: 16.99,
                in_stock: true,
                pages: 476,
                publisher: "Ballantine Books"
            }
        ];

        // Clear existing data and insert new sample data
        await this.books.deleteMany({});
        const result = await this.books.insertMany(sampleBooks);
        console.log(`✅ ${result.insertedCount} sample books inserted\n`);
    }

    async task2BasicCRUD() {
        console.log("\n1. 🔍 FIND ALL BOOKS IN 'FANTASY' GENRE:");
        const fantasyBooks = await this.books.find({ genre: "Fantasy" }).toArray();
        fantasyBooks.forEach(book => {
            console.log(`   - "${book.title}" by ${book.author}`);
        });

        console.log("\n2. 📅 FIND BOOKS PUBLISHED AFTER 1950:");
        const recentBooks = await this.books.find({ 
            published_year: { $gt: 1950 } 
        }).toArray();
        recentBooks.forEach(book => {
            console.log(`   - "${book.title}" (${book.published_year})`);
        });

        console.log("\n3. ✍️ FIND BOOKS BY 'J.R.R. TOLKIEN':");
        const tolkienBooks = await this.books.find({ 
            author: "J.R.R. Tolkien" 
        }).toArray();
        tolkienBooks.forEach(book => {
            console.log(`   - "${book.title}" (${book.published_year})`);
        });

        console.log("\n4. 💰 UPDATE PRICE OF '1984' TO $12.99:");
        const updateResult = await this.books.updateOne(
            { title: "1984" },
            { $set: { price: 12.99 } }
        );
        console.log(`   Modified ${updateResult.modifiedCount} document(s)`);

        console.log("\n5. 🗑️ DELETE 'THE DA VINCI CODE':");
        const deleteResult = await this.books.deleteOne({ 
            title: "The Da Vinci Code" 
        });
        console.log(`   Deleted ${deleteResult.deletedCount} document(s)`);
    }

    // TASK 3: Advanced Queries
    async task3AdvancedQueries() {
        console.log("\n\n📊 TASK 3: ADVANCED QUERIES");
        console.log("=".repeat(50));

        console.log("\n1. 🔍 BOOKS IN STOCK AND PUBLISHED AFTER 2010:");
        const inStockRecent = await this.books.find({
            in_stock: true,
            published_year: { $gt: 2010 }
        }).toArray();
        inStockRecent.forEach(book => {
            console.log(`   - "${book.title}" by ${book.author} (${book.published_year}) - $${book.price}`);
        });

        console.log("\n2. 🎯 PROJECTION - TITLE, AUTHOR, PRICE ONLY:");
        const projectedBooks = await this.books.find(
            { genre: "Fiction" },
            { 
                projection: { 
                    title: 1, 
                    author: 1, 
                    price: 1,
                    _id: 0
                } 
            }
        ).limit(5).toArray();
        projectedBooks.forEach(book => {
            console.log(`   - "${book.title}" by ${book.author} - $${book.price}`);
        });

        console.log("\n3. 📊 SORTING BY PRICE:");
        console.log("   🔼 Ascending (Low to High):");
        const ascending = await this.books.find(
            {},
            { projection: { title: 1, price: 1, _id: 0 } }
        ).sort({ price: 1 }).limit(3).toArray();
        ascending.forEach(book => {
            console.log(`      $${book.price} - "${book.title}"`);
        });

        console.log("   🔽 Descending (High to Low):");
        const descending = await this.books.find(
            {},
            { projection: { title: 1, price: 1, _id: 0 } }
        ).sort({ price: -1 }).limit(3).toArray();
        descending.forEach(book => {
            console.log(`      $${book.price} - "${book.title}"`);
        });

        console.log("\n4. 📄 PAGINATION (5 BOOKS PER PAGE):");
        const booksPerPage = 5;
        
        console.log("   Page 1:");
        const page1 = await this.books.find(
            {},
            { projection: { title: 1, author: 1, _id: 0 } }
        ).sort({ title: 1 }).limit(booksPerPage).toArray();
        page1.forEach(book => {
            console.log(`      - "${book.title}" - ${book.author}`);
        });

        console.log("   Page 2:");
        const page2 = await this.books.find(
            {},
            { projection: { title: 1, author: 1, _id: 0 } }
        ).sort({ title: 1 }).skip(booksPerPage).limit(booksPerPage).toArray();
        page2.forEach(book => {
            console.log(`      - "${book.title}" - ${book.author}`);
        });
    }

    // TASK 4: Aggregation Pipeline
    async task4AggregationPipelines() {
        console.log("\n\n📈 TASK 4: AGGREGATION PIPELINES");
        console.log("=".repeat(50));

        console.log("\n1. 📊 AVERAGE PRICE BY GENRE:");
        const avgPriceByGenre = await this.books.aggregate([
            {
                $group: {
                    _id: "$genre",
                    averagePrice: { $avg: "$price" },
                    bookCount: { $sum: 1 }
                }
            },
            { $sort: { averagePrice: -1 } }
        ]).toArray();
        avgPriceByGenre.forEach(genre => {
            console.log(`   ${genre._id}: $${genre.averagePrice.toFixed(2)} (${genre.bookCount} books)`);
        });

        console.log("\n2. 👨‍💼 AUTHOR WITH MOST BOOKS:");
        const authorsByBookCount = await this.books.aggregate([
            {
                $group: {
                    _id: "$author",
                    bookCount: { $sum: 1 }
                }
            },
            { $sort: { bookCount: -1 } },
            { $limit: 3 }
        ]).toArray();
        authorsByBookCount.forEach((author, index) => {
            const rank = index === 0 ? "🥇" : index === 1 ? "🥈" : "🥉";
            console.log(`   ${rank} ${author._id}: ${author.bookCount} books`);
        });

        console.log("\n3. 📅 BOOKS BY PUBLICATION DECADE:");
        const booksByDecade = await this.books.aggregate([
            {
                $project: {
                    title: 1,
                    published_year: 1,
                    decade: {
                        $subtract: [
                            "$published_year",
                            { $mod: ["$published_year", 10] }
                        ]
                    }
                }
            },
            {
                $group: {
                    _id: "$decade",
                    bookCount: { $sum: 1 },
                    books: { $push: "$title" }
                }
            },
            { $sort: { _id: 1 } }
        ]).toArray();
        booksByDecade.forEach(decade => {
            console.log(`   ${decade._id}s: ${decade.bookCount} books`);
        });
    }

    // TASK 5: Indexing
    async task5Indexing() {
        console.log("\n\n⚡ TASK 5: INDEXING");
        console.log("=".repeat(50));

        console.log("\n1. 🔧 CREATING INDEXES:");
        
        // Create single index on title field
        await this.books.createIndex({ title: 1 });
        console.log("   ✅ Single index created on 'title' field");

        // Create compound index on author and published_year
        await this.books.createIndex({ author: 1, published_year: -1 });
        console.log("   ✅ Compound index created on 'author' and 'published_year'");

        console.log("\n2. 🚀 PERFORMANCE COMPARISON:");

        // Test without index (simulated by hinting no index)
        console.log("   Without index (Collection Scan):");
        const withoutIndex = await this.books.find({ title: "The Hobbit" })
            .hint({ $natural: 1 }) // Force collection scan
            .explain("executionStats");
        console.log(`      Documents examined: ${withoutIndex.executionStats.totalDocsExamined}`);
        console.log(`      Execution time: ${withoutIndex.executionStats.executionTimeMillis}ms`);

        // Test with index
        console.log("   With index on 'title':");
        const withIndex = await this.books.find({ title: "The Hobbit" })
            .explain("executionStats");
        console.log(`      Documents examined: ${withIndex.executionStats.totalDocsExamined}`);
        console.log(`      Execution time: ${withIndex.executionStats.executionTimeMillis}ms`);

        // Test compound index
        console.log("   With compound index (author + year):");
        const compoundQuery = await this.books.find({ 
            author: "Andy Weir",
            published_year: { $gt: 2010 }
        }).explain("executionStats");
        console.log(`      Documents examined: ${compoundQuery.executionStats.totalDocsExamined}`);
        console.log(`      Execution time: ${compoundQuery.executionStats.executionTimeMillis}ms`);
    }

    // Main method to run all tasks
    async runAllTasks() {
        if (!await this.connect()) {
            return;
        }

        try {
            await this.initializeSampleData();
            await this.task2BasicCRUD();
            await this.task3AdvancedQueries();
            await this.task4AggregationPipelines();
            await this.task5Indexing();

            console.log("\n🎉 ALL TASKS COMPLETED SUCCESSFULLY!");
            console.log("=====================================");

        } catch (error) {
            console.error('❌ Error during execution:', error);
        } finally {
            await this.disconnect();
        }
    }
}

// Run the application
async function main() {
    const bookQueries = new BookQueries();
    await bookQueries.runAllTasks();
}

// Export for use in other files
module.exports = { BookQueries };

// Run if this file is executed directly
if (require.main === module) {
    main().catch(console.error);
}
