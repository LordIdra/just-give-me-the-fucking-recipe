CREATE TABLE IF NOT EXISTS word (
    id       INT NOT NULL AUTO_INCREMENT,
    word     TEXT NOT NULL UNIQUE,
    parent   INT,
    priority INT NOT NULL,
    status   ENUM (
        "WAITING_FOR_GENERATION", "GENERATING", "GENERATION_FAILED",  "GENERATION_COMPLETE",
        "WAITING_FOR_CLASSIFICATION", "CLASSIFYING", "CLASSIFICATION_FAILED", "CLASSIFIED_AS_INVALID",
        "WAITING_FOR_SEARCH", "SEARCHING", "SEARCH_FAILED", "SEARCH_COMPLETE"
    ) NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (parent) REFERENCES word(id)
);

CREATE TABLE IF NOT EXISTS link_blacklist (
    id       INT NOT NULL AUTO_INCREMENT,
    word     TEXT NOT NULL UNIQUE,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS page (
    id              INT NOT NULL AUTO_INCREMENT,
    link            TEXT NOT NULL,
    word_source     INT,
    follower_source INT,
    content         MEDIUMTEXT,
    schema          MEDIUMTEXT,
    priority        INT NOT NULL,
    status          ENUM (
        "WAITING_FOR_DOWNLOAD", "DOWNLOADING", "DOWNLOAD_FAILED", 
        "WAITING_FOR_EXTRACTION", "EXTRACTING", "EXTRACTION_FAILED", 
        "WAITING_FOR_PARSING", "PARSING", "PARSING_INCOMPLETE_RECIPE",
        "WAITING_FOR_FOLLOWING", "FOLLOWING", "FOLLOWING_COMPLETE", "FOLLOWING_FAILED"
    ) NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS recipe (
    id                 INT NOT NULL AUTO_INCREMENT,
    page               INT NOT NULL,
    title              TEXT,
    description        TEXT,
    date               DATE,
    rating             FLOAT,
    rating_count       INT,
    prep_time_seconds  INT,
    cook_time_seconds  INT,
    total_time_seconds INT,
    servings           TEXT,
    calories           FLOAT,
    carbohydrates      FLOAT,
    cholesterol        FLOAT,
    fat                FLOAT,
    fiber              FLOAT,
    protein            FLOAT,
    saturated_fat      FLOAT,
    sodium             FLOAT,
    sugar              FLOAT,
    PRIMARY KEY (id),
    CONSTRAINT UniqueRecipe UNIQUE (title, description),
    FOREIGN KEY (page) REFERENCES page(id)
);

CREATE TABLE IF NOT EXISTS keyword (
    id      INT NOT NULL AUTO_INCREMENT,
    keyword TEXT NOT NULL UNIQUE,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS recipe_keyword (
    recipe  INT NOT NULL,
    keyword INT NOT NULL,
    PRIMARY KEY (recipe, keyword),
    FOREIGN KEY (recipe) REFERENCES recipe(id),
    FOREIGN KEY (keyword) REFERENCES keyword(id)
);

CREATE TABLE IF NOT EXISTS author (
    id   INT NOT NULL AUTO_INCREMENT,
    name TEXT NOT NULL,
    link TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS recipe_author (
    recipe INT NOT NULL,
    author INT NOT NULL,
    PRIMARY KEY (recipe, author),
    FOREIGN KEY (recipe) REFERENCES recipe(id),
    FOREIGN KEY (author) REFERENCES author(id)
);

CREATE TABLE IF NOT EXISTS image (
    id    INT NOT NULL AUTO_INCREMENT,
    image TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS recipe_image (
    recipe INT NOT NULL,
    image  INT NOT NULL,
    PRIMARY KEY (recipe, image),
    FOREIGN KEY (recipe) REFERENCES recipe(id),
    FOREIGN KEY (image) REFERENCES image(id)
);

CREATE TABLE IF NOT EXISTS ingredient (
    id         INT NOT NULL AUTO_INCREMENT,
    ingredient TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS recipe_ingredient (
    recipe     INT NOT NULL,
    ingredient INT NOT NULL,
    PRIMARY KEY (recipe, ingredient),
    FOREIGN KEY (recipe) REFERENCES recipe(id),
    FOREIGN KEY (ingredient) REFERENCES ingredient(id)
);

