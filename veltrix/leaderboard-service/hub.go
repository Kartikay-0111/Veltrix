package main

import (
	"log"
	"sync"

	"github.com/gorilla/websocket"
)

// ─────────────────────────────────────────────────────────────────────────────
// Hub — manages all active WebSocket connections
//
// Design: shared-nothing between connections.
// Each client has a buffered channel. The Hub never blocks on a slow client —
// if the channel is full, the client is dropped (browser will reconnect).
//
// Broadcast sends raw HTML strings — HTMX handles the DOM swap in the browser.
// ─────────────────────────────────────────────────────────────────────────────

const clientSendBuffer = 64 // messages buffered per client before drop

type Client struct {
	conn *websocket.Conn
	send chan []byte
}

type Hub struct {
	clients   map[*Client]struct{}
	mu        sync.RWMutex
	broadcast chan []byte
	register  chan *Client
	unregister chan *Client
}

func newHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]struct{}),
		broadcast:  make(chan []byte, 256),
		register:   make(chan *Client, 32),
		unregister: make(chan *Client, 32),
	}
}

// Run — event loop. Call in a goroutine.
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = struct{}{}
			h.mu.Unlock()
			log.Printf("[Hub] Client connected. Total: %d", len(h.clients))

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
			h.mu.Unlock()
			log.Printf("[Hub] Client disconnected. Total: %d", len(h.clients))

		case html := <-h.broadcast:
			h.mu.RLock()
			for client := range h.clients {
				select {
				case client.send <- html:
					// delivered
				default:
					// client too slow — drop it, browser will reconnect
					h.mu.RUnlock()
					h.unregister <- client
					h.mu.RLock()
				}
			}
			h.mu.RUnlock()
		}
	}
}

// Broadcast sends an HTML string to all connected browsers.
func (h *Hub) Broadcast(html []byte) {
	h.broadcast <- html
}

// writePump — pumps messages from client.send channel to the WebSocket.
// Runs in a goroutine per client.
func (c *Client) writePump(hub *Hub) {
	defer func() {
		hub.unregister <- c
		c.conn.Close()
	}()

	for html := range c.send {
		if err := c.conn.WriteMessage(websocket.TextMessage, html); err != nil {
			return
		}
	}
}

// readPump — drains incoming messages (HTMX sends pings, we ignore them).
// Required by gorilla/websocket — must read or the connection backs up.
func (c *Client) readPump(hub *Hub) {
	defer func() {
		hub.unregister <- c
		c.conn.Close()
	}()

	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			return
		}
	}
}
