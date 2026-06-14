package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// =================== RESP Protocol ====================

type RespType int

const (
	RespString RespType = iota
	RespError
	RespInt
	RespBulk
	RespArray
	RespNull
)

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// Parse RESP command from reader
func parseCommand(r *bufio.Reader) ([]string, bool, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, false, err
	}
	if line == "" || line[0] != '*' {
		return nil, false, fmt.Errorf("expected array")
	}
	count, _ := strconv.Atoi(line[1:])
	args := make([]string, count)
	for i := 0; i < count; i++ {
		line, err = readLine(r)
		if err != nil || line[0] != '$' {
			return nil, false, err
		}
		arg, err := readLine(r)
		if err != nil {
			return nil, false, err
		}
		args[i] = arg
	}
	return args, true, nil
}

// =================== RESP Writers ====================

func writeOK(w io.Writer)            { w.Write([]byte("+OK\r\n")) }
func writeNil(w io.Writer)           { w.Write([]byte("$-1\r\n")) }
func writePong(w io.Writer)          { w.Write([]byte("+PONG\r\n")) }
func writeInt(w io.Writer, n int64)  { fmt.Fprintf(w, ":%d\r\n", n) }
func writeError(w io.Writer, msg string) { fmt.Fprintf(w, "-%s\r\n", msg) }
func writeBulk(w io.Writer, s string) {
	fmt.Fprintf(w, "$%d\r\n%s\r\n", len(s), s)
}
func writeArray(w io.Writer, items []string) {
	fmt.Fprintf(w, "*%d\r\n", len(items))
	for _, item := range items {
		writeBulk(w, item)
	}
}

// =================== Store ====================

type EntryType int

const (
	TypeString EntryType = iota
	TypeList
	TypeHash
	TypeSet
	TypeZSet
)

type ZSetItem struct {
	Score  float64
	Member string
}

type Entry struct {
	Type      EntryType
	Str       string
	List      []string
	Hash      map[string]string
	Set       map[string]bool
	ZSetBySc  []ZSetItem // sorted by score
	ZSetByMem map[string]float64
	ExpireAt  time.Time // zero = no expire
}

type Store struct {
	mu   sync.RWMutex
	data map[string]*Entry
}

func NewStore() *Store { return &Store{data: make(map[string]*Entry)} }

func (s *Store) isExpired(e *Entry) bool {
	return !e.ExpireAt.IsZero() && time.Now().After(e.ExpireAt)
}

// ---- Key Commands ----

func (s *Store) Del(keys []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, k := range keys {
		if _, ok := s.data[k]; ok {
			delete(s.data, k)
			count++
		}
	}
	return count
}

func (s *Store) Exists(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	return ok && !s.isExpired(e)
}

func (s *Store) Keys(pattern string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []string
	for k, e := range s.data {
		if s.isExpired(e) { continue }
		if pattern == "*" || k == pattern {
			result = append(result, k)
		}
	}
	return result
}

func (s *Store) Expire(key string, sec int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[key]
	if !ok { return false }
	e.ExpireAt = time.Now().Add(time.Duration(sec) * time.Second)
	return true
}

func (s *Store) TTL(key string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	if !ok { return -2 }
	if e.ExpireAt.IsZero() { return -1 }
	remain := int64(time.Until(e.ExpireAt).Seconds())
	if remain < 0 { return -2 }
	return remain
}

// ---- String ----

func (s *Store) Set(key, val string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = &Entry{Type: TypeString, Str: val}
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	if !ok || s.isExpired(e) || e.Type != TypeString { return "", false }
	return e.Str, true
}

func (s *Store) Incr(key string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[key]
	if !ok {
		s.data[key] = &Entry{Type: TypeString, Str: "0"}
		e = s.data[key]
	}
	n, err := strconv.ParseInt(e.Str, 10, 64)
	if err != nil { return 0, err }
	n++
	e.Str = strconv.FormatInt(n, 10)
	return n, nil
}

// ---- List ----

func (s *Store) LPush(key string, vals []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[key]
	if !ok { e = &Entry{Type: TypeList}; s.data[key] = e }
	e.List = append(vals, e.List...)
	return len(e.List)
}

func (s *Store) LRange(key string, start, stop int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	if !ok || e.Type != TypeList { return nil }
	l := len(e.List)
	if start < 0 { start = l + start }
	if stop < 0 { stop = l + stop }
	if start < 0 { start = 0 }
	if stop >= l { stop = l - 1 }
	if start > stop { return nil }
	return e.List[start : stop+1]
}

func (s *Store) LLen(key string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	if !ok { return 0 }
	return len(e.List)
}

// ---- Hash ----

func (s *Store) HSet(key, field, val string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[key]
	if !ok { e = &Entry{Type: TypeHash, Hash: make(map[string]string)}; s.data[key] = e }
	_, existed := e.Hash[field]
	e.Hash[field] = val
	if existed { return 0 }
	return 1
}

func (s *Store) HGet(key, field string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	if !ok || e.Type != TypeHash { return "", false }
	v, ok := e.Hash[field]
	return v, ok
}

func (s *Store) HGetAll(key string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	if !ok || e.Type != TypeHash { return nil }
	var res []string
	for k, v := range e.Hash {
		res = append(res, k, v)
	}
	return res
}

// ---- Set ----

func (s *Store) SAdd(key string, members []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[key]
	if !ok { e = &Entry{Type: TypeSet, Set: make(map[string]bool)}; s.data[key] = e }
	added := 0
	for _, m := range members {
		if !e.Set[m] { added++; e.Set[m] = true }
	}
	return added
}

func (s *Store) SMembers(key string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	if !ok || e.Type != TypeSet { return nil }
	var res []string
	for m := range e.Set { res = append(res, m) }
	return res
}

// ---- ZSet ----

func (s *Store) ZAdd(key string, score float64, member string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[key]
	if !ok {
		e = &Entry{Type: TypeZSet, ZSetByMem: make(map[string]float64)}
		s.data[key] = e
	}
	oldScore, existed := e.ZSetByMem[member]
	if existed {
		// Remove old entry
		for i, item := range e.ZSetBySc {
			if item.Member == member { e.ZSetBySc = append(e.ZSetBySc[:i], e.ZSetBySc[i+1:]...); break }
		}
		_ = oldScore
	}
	// Insert sorted
	pos := 0
	for pos < len(e.ZSetBySc) && e.ZSetBySc[pos].Score < score { pos++ }
	e.ZSetBySc = append(e.ZSetBySc, ZSetItem{})
	copy(e.ZSetBySc[pos+1:], e.ZSetBySc[pos:])
	e.ZSetBySc[pos] = ZSetItem{Score: score, Member: member}
	e.ZSetByMem[member] = score
	if existed { return 0 }
	return 1
}

func (s *Store) ZRange(key string, start, stop int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	if !ok || e.Type != TypeZSet { return nil }
	l := len(e.ZSetBySc)
	if start < 0 { start = l + start }
	if stop < 0 { stop = l + stop }
	if start < 0 { start = 0 }
	if stop >= l { stop = l - 1 }
	if start > stop { return nil }
	var res []string
	for i := start; i <= stop; i++ { res = append(res, e.ZSetBySc[i].Member) }
	return res
}

// =================== Server ====================

var activeConns int64

func handle(conn net.Conn, store *Store) {
	defer conn.Close()
	atomic.AddInt64(&activeConns, 1)
	defer atomic.AddInt64(&activeConns, -1)

	reader := bufio.NewReader(conn)

	for {
		args, _, err := parseCommand(reader)
		if err != nil {
			if err != io.EOF { writeError(conn, "ERR "+err.Error()) }
			return
		}
		cmd := strings.ToUpper(args[0])

		switch cmd {
		case "PING":
			writePong(conn)
		case "SET":
			if len(args) >= 3 { store.Set(args[1], args[2]); writeOK(conn) }
		case "GET":
			if len(args) >= 2 {
				v, ok := store.Get(args[1])
				if ok { writeBulk(conn, v) } else { writeNil(conn) }
			}
		case "DEL":
			if len(args) >= 2 { writeInt(conn, int64(store.Del(args[1:]))) }
		case "EXISTS":
			if len(args) >= 2 { writeInt(conn, boolToInt(store.Exists(args[1]))) }
		case "KEYS":
			if len(args) >= 2 { writeArray(conn, store.Keys(args[1])) }
		case "EXPIRE":
			if len(args) >= 3 { sec, _ := strconv.Atoi(args[2]); writeInt(conn, boolToInt(store.Expire(args[1], sec))) }
		case "TTL":
			if len(args) >= 2 { writeInt(conn, store.TTL(args[1])) }
		case "INCR":
			if len(args) >= 2 {
				n, err := store.Incr(args[1])
				if err != nil { writeError(conn, "ERR "+err.Error()) } else { writeInt(conn, n) }
			}
		case "LPUSH":
			if len(args) >= 3 { writeInt(conn, int64(store.LPush(args[1], args[2:]))) }
		case "LRANGE":
			if len(args) >= 4 {
				s, _ := strconv.Atoi(args[2]); e, _ := strconv.Atoi(args[3])
				res := store.LRange(args[1], s, e)
				if res == nil { res = []string{} }
				writeArray(conn, res)
			}
		case "LLEN":
			if len(args) >= 2 { writeInt(conn, int64(store.LLen(args[1]))) }
		case "HSET":
			if len(args) >= 4 { writeInt(conn, int64(store.HSet(args[1], args[2], args[3]))) }
		case "HGET":
			if len(args) >= 3 {
				v, ok := store.HGet(args[1], args[2])
				if ok { writeBulk(conn, v) } else { writeNil(conn) }
			}
		case "HGETALL":
			if len(args) >= 2 { writeArray(conn, store.HGetAll(args[1])) }
		case "SADD":
			if len(args) >= 3 { writeInt(conn, int64(store.SAdd(args[1], args[2:]))) }
		case "SMEMBERS":
			if len(args) >= 2 { writeArray(conn, store.SMembers(args[1])) }
		case "ZADD":
			if len(args) >= 4 { s, _ := strconv.ParseFloat(args[2], 64); writeInt(conn, int64(store.ZAdd(args[1], s, args[3]))) }
		case "ZRANGE":
			if len(args) >= 4 { s, _ := strconv.Atoi(args[2]); e, _ := strconv.Atoi(args[3]); writeArray(conn, store.ZRange(args[1], s, e)) }
		case "DBSIZE":
			// Quick count
			store.mu.RLock()
			n := len(store.data)
			store.mu.RUnlock()
			writeInt(conn, int64(n))
		default:
			writeError(conn, "ERR unknown command '"+cmd+"'")
		}
	}
}

func boolToInt(b bool) int64 { if b { return 1 }; return 0 }

func main() {
	port := "6379"
	if p := os.Getenv("PORT"); p != "" { port = p }

	store := NewStore()

	ln, err := net.Listen("tcp", ":"+port)
	if err != nil { log.Fatal(err) }

	log.Printf("Mini Redis (Go) listening on :%s", port)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go func() { <-sig; log.Println("Shutting down..."); os.Exit(0) }()

	for {
		conn, err := ln.Accept()
		if err != nil { continue }
		go handle(conn, store)
	}
}
