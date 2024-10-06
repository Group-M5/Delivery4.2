DROP DATABASE IF EXISTS Uno;
CREATE DATABASE Uno;
USE Uno;

/* Entities Player & Game + Additional tables*/

CREATE TABLE Player (
	PlayerID INT NOT NULL AUTO_INCREMENT,
	Username VARCHAR(30) NOT NULL,
	PPassword VARCHAR(30),
	Points INT,
	GamesWon INT,
	PRIMARY KEY (PlayerID),
	UNIQUE (Username) -- Acts as a secondary key, so there won't be two iddentical usernames
);

CREATE TABLE Game (
	GameID INT NOT NULL AUTO_INCREMENT,
	GameDate VARCHAR(10),
	Duration INT,
	Winner VARCHAR(30),
	NumOfMoves INT,
	GameState VARCHAR(15), -- Not started , On-course, Ended
	PRIMARY KEY (GameID)
);

CREATE TABLE Cards (
	CardID INT NOT NULL AUTO_INCREMENT,
	CNumber INT,
	CColor VARCHAR(10) NOT NULL,
	CType VARCHAR(10) NOT NULL,
	CAction VARCHAR(10),
	PRIMARY KEY (CardID)
);

CREATE TABLE Deck (
	DeckID INT,
	GameID INT,
	PlayerID INT,
	CardID INT,
	PRIMARY KEY (GameID, PlayerID, CardID),
	FOREIGN KEY (GameID) REFERENCES Game(GameID),
	FOREIGN KEY (PlayerID) REFERENCES Player(PlayerID),
	FOREIGN KEY (CardID) REFERENCES Cards(CardID)
);

CREATE TABLE Turns (
	TurnID INT NOT NULL AUTO_INCREMENT,
	GameID INT NOT NULL,
	PlayerID INT NOT NULL,
	TurnDirection VARCHAR(20), -- Clockwise, Counter-clockwise
	SkipTurn BOOLEAN,
	PRIMARY KEY (TurnID),
	FOREIGN KEY (GameID) REFERENCES Game(GameID),
	FOREIGN KEY (PlayerID) REFERENCES Player(PlayerID)
);

-- Insert data into Player table
INSERT INTO Player (PlayerID, Username, PPassword, Points, GamesWon)
VALUES
(1, 'player1', 'password1', 0, 0),
(2, 'player2', 'password2', 0, 0),
(3, 'player3', 'password3', 0, 0);

-- Insert data into Game table
INSERT INTO Game (GameID, GameDate, Duration, Winner, NumOfMoves, GameState)
VALUES
(1, '2022-01-01', 30, NULL, 0, 'Not started'),
(2, '2022-01-02', 45, NULL, 0, 'Not started');

-- Insert data into Cards table
INSERT INTO Cards (CardID, CNumber, CColor, CType, CAction)
VALUES
(1, 1, 'Red', 'Number', NULL),
(2, 2, 'Red', 'Number', NULL),
(3, 3, 'Red', 'Number', NULL),
(4, 1, 'Green', 'Number', NULL),
(5, 2, 'Green', 'Number', NULL),
(6, 3, 'Green', 'Number', NULL),
(7, NULL, 'Blue', 'Action', 'Draw 2'),
(8, NULL, 'Blue', 'Action', 'Draw 4'),
(9, NULL, 'Blue', 'Action', 'Block'),
(10, NULL, 'Blue', 'Action', 'Reverse'),
(11, NULL, 'Yellow', 'Action', 'Draw 2'),
(12, NULL, 'Yellow', 'Action', 'Draw 4'),
(13, NULL, 'Yellow', 'Action', 'Block'),
(14, NULL, 'Yellow', 'Action', 'Reverse');

-- Insert data into Deck table
INSERT INTO Deck (GameID, PlayerID, CardID)
VALUES
(1, 1, 1),
(1, 1, 2),
(1, 1, 3),
(2, 2, 4),
(2, 2, 5),
(2, 2, 6),
(2, 2, 7),
(1, 3, 8),
(1, 3, 9);

-- Insert data into Turns table
INSERT INTO Turns (TurnID, GameID, PlayerID, TurnDirection, SkipTurn)
VALUES
(1, 1, 1, 'Clockwise', FALSE),
(2, 1, 2, 'Clockwise', FALSE),
(3, 2, 1, 'Counter-clockwise', TRUE),
(4, 2, 3, 'Counter-clockwise', FALSE);






















