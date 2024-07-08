package client

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/netip"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/pkg/errors"

	"github.com/RomiChan/syncx"
	"github.com/gorilla/websocket"

	"github.com/Mrs4s/MiraiGo/binary"
	"github.com/Mrs4s/MiraiGo/client/internal/auth"
	"github.com/Mrs4s/MiraiGo/client/internal/highway"
	"github.com/Mrs4s/MiraiGo/client/internal/intern"
	"github.com/Mrs4s/MiraiGo/client/internal/network"
	"github.com/Mrs4s/MiraiGo/client/internal/oicq"
	"github.com/Mrs4s/MiraiGo/client/pb/msg"
	"github.com/Mrs4s/MiraiGo/message"
	"github.com/Mrs4s/MiraiGo/utils"
	"github.com/Sora233/MiraiGo-Template/config"
)

type QQClient struct {
	Uin         int64
	PasswordMd5 [16]byte

	stat Statistics
	once sync.Once

	// option
	AllowSlider        bool
	UseFragmentMessage bool

	// account info
	Online        atomic.Bool
	Nickname      string
	Age           uint16
	Gender        uint16
	FriendList    []*FriendInfo
	GroupList     []*GroupInfo
	OnlineClients []*OtherClientInfo
	QiDian        *QiDianAccountInfo
	GuildService  *GuildService

	// protocol public field
	SequenceId  atomic.Int32
	SessionId   []byte
	TCP         *network.TCPClient // todo: combine other protocol state into one struct
	ConnectTime time.Time

	transport *network.Transport
	oicq      *oicq.Codec
	logger    Logger

	// internal state
	handlers        syncx.Map[uint16, *handlerInfo]
	waiters         syncx.Map[string, func(any, error)]
	initServerOnce  sync.Once
	servers         []netip.AddrPort
	currServerIndex int
	retryTimes      int
	alive           bool

	// session info
	qwebSeq        atomic.Int64
	sig            *auth.SigInfo
	highwaySession *highway.Session
	// pwdFlag        bool
	// timeDiff       int64

	// address
	// otherSrvAddrs   []string
	// fileStorageInfo *jce.FileStoragePushFSSvcList

	// event handles
	eventHandlers                     eventHandlers
	PrivateMessageEvent               EventHandle[*message.PrivateMessage]
	TempMessageEvent                  EventHandle[*TempMessageEvent]
	GroupMessageEvent                 EventHandle[*message.GroupMessage]
	SelfPrivateMessageEvent           EventHandle[*message.PrivateMessage]
	SelfGroupMessageEvent             EventHandle[*message.GroupMessage]
	GroupMuteEvent                    EventHandle[*GroupMuteEvent]
	GroupMessageRecalledEvent         EventHandle[*GroupMessageRecalledEvent]
	FriendMessageRecalledEvent        EventHandle[*FriendMessageRecalledEvent]
	GroupJoinEvent                    EventHandle[*GroupInfo]
	GroupLeaveEvent                   EventHandle[*GroupLeaveEvent]
	GroupMemberJoinEvent              EventHandle[*MemberJoinGroupEvent]
	GroupMemberLeaveEvent             EventHandle[*MemberLeaveGroupEvent]
	MemberCardUpdatedEvent            EventHandle[*MemberCardUpdatedEvent]
	GroupNameUpdatedEvent             EventHandle[*GroupNameUpdatedEvent]
	GroupMemberPermissionChangedEvent EventHandle[*MemberPermissionChangedEvent]
	GroupInvitedEvent                 EventHandle[*GroupInvitedRequest]
	UserWantJoinGroupEvent            EventHandle[*UserJoinGroupRequest]
	NewFriendEvent                    EventHandle[*NewFriendEvent]
	NewFriendRequestEvent             EventHandle[*NewFriendRequest]
	DisconnectedEvent                 EventHandle[*ClientDisconnectedEvent]
	GroupNotifyEvent                  EventHandle[INotifyEvent]
	FriendNotifyEvent                 EventHandle[INotifyEvent]
	MemberSpecialTitleUpdatedEvent    EventHandle[*MemberSpecialTitleUpdatedEvent]
	GroupDigestEvent                  EventHandle[*GroupDigestEvent]
	OtherClientStatusChangedEvent     EventHandle[*OtherClientStatusChangedEvent]
	OfflineFileEvent                  EventHandle[*OfflineFileEvent]
	GroupDisbandEvent                 EventHandle[*GroupDisbandEvent]
	DeleteFriendEvent                 EventHandle[*DeleteFriendEvent]

	// message state
	msgSvcCache            *utils.Cache[unit]
	lastC2CMsgTime         int64
	transCache             *utils.Cache[unit]
	groupSysMsgCache       *GroupSystemMessages
	msgBuilders            syncx.Map[int32, *messageBuilder]
	onlinePushCache        *utils.Cache[unit]
	heartbeatEnabled       bool
	requestPacketRequestID atomic.Int32
	groupSeq               atomic.Int32
	friendSeq              atomic.Int32
	highwayApplyUpSeq      atomic.Int32

	groupListLock sync.Mutex
	//增加的字段
	ws                 *websocket.Conn
	responseCh         map[string]chan *T
	disconnectChan     chan bool
	currentEcho        string
	limiterMessageSend *RateLimiter
}

// 新增
type GroupData struct {
	GroupCreateTime int64  `json:"group_create_time"`
	GroupID         int64  `json:"group_id"`
	GroupLevel      int    `json:"group_level"`
	GroupName       string `json:"group_name"`
	MaxMemberCount  int    `json:"max_member_count"`
	MemberCount     int    `json:"member_count"`
	// ... and other fields ...
}

// 新增
type ResponseGroupsData struct {
	Data    []GroupData `json:"data"`
	Message string      `json:"message"`
	Retcode int         `json:"retcode"`
	Status  string      `json:"status"`
	Echo    string      `json:"echo"`
}

// 新增
type ResponseGroupData struct {
	Data    GroupData `json:"data"`
	Message string    `json:"message"`
	Retcode int       `json:"retcode"`
	Status  string    `json:"status"`
	Echo    string    `json:"echo"`
}

// 好友信息请求返回
type ResponseFriendList struct {
	Status  string       `json:"status"`
	Retcode int          `json:"retcode"`
	Message string       `json:"message"`
	Data    []FriendData `json:"data"`
	Wording string       `json:"wording"`
}

// 好友信息
type FriendData struct {
	UserID   int64  `json:"user_id"`
	NickName string `json:"nickname"`
	Remark   string `json:"remark"`
	Sex      string `json:"sex"`
	Level    int    `json:"level"`
}

// 群成员信息
type MemberData struct {
	GroupID         int64       `json:"group_id"`
	UserID          int64       `json:"user_id"`
	Nickname        string      `json:"nickname"`
	Card            string      `json:"card"`
	Sex             string      `json:"sex"`
	Age             int         `json:"age"`
	Area            string      `json:"area"`
	Level           interface{} `json:"level"`
	QQLevel         int16       `json:"qq_level"`
	JoinTime        int64       `json:"join_time"`
	LastSentTime    int64       `json:"last_sent_time"`
	TitleExpireTime int64       `json:"title_expire_time"`
	Unfriendly      bool        `json:"unfriendly"`
	CardChangeable  bool        `json:"card_changeable"`
	IsRobot         bool        `json:"is_robot"`
	ShutUpTimestamp int64       `json:"shut_up_timestamp"`
	Role            string      `json:"role"`
	Title           string      `json:"title"`
}

// 群成员请求返回
type ResponseGroupMemberData struct {
	Status  string       `json:"status"`
	Retcode int          `json:"retcode"`
	Message string       `json:"message"`
	Wording string       `json:"wording"`
	Data    []MemberData `json:"data"`
}

// 新增
type BasicMessage struct {
	Echo string `json:"echo"`
	// Data json.RawMessage `json:"data"`
}

// 发送消息返回
type ResponseSendMessage struct {
	Status  string `json:"status"`
	Retcode int    `json:"retcode"`
	Data    struct {
		MessageID int32 `json:"message_id"`
	}
	Message string `json:"message"`
	Wording string `json:"wording"`
}

// set指令返回
type ResponseSetGroupLeave struct {
	Status  string `json:"status"`
	Retcode int    `json:"retcode"`
	Message string `json:"message"`
	Wording string `json:"wording"`
	Echo    string `json:"echo"`
}

// 发送消息返回
type SendResp struct {
	RetMSG *message.GroupMessage
	Error  error
}

// 查陌生人返回
type ResponseGetStrangerInfo struct {
	Status  string       `json:"status"`
	Retcode int          `json:"retcode"`
	Data    StrangerInfo `json:"data"`
	Message string       `json:"message"`
	Wording string       `json:"wording"`
	Echo    string       `json:"echo"`
}

type StrangerInfo struct {
	UID       string `json:"uid"`
	Uin       string `json:"uin"`
	Nick      string `json:"nick"`
	Remark    string `json:"remark"`
	LongNick  string `json:"longNick"`
	Sex       string `json:"sex"`
	ShengXiao int    `json:"shengXiao"`
	RegTime   int64  `json:"regTime"`
	QQLevel   struct {
		CrownNum int `json:"crownNum"`
		SunNum   int `json:"sunNum"`
		MoonNum  int `json:"moonNum"`
		StarNum  int `json:"starNum"`
	} `json:"qqLevel"`
	Age   int `json:"age"`
	Level int `json:"level"`
}

// 卡片消息
type CardMessage struct {
	App    string `json:"app"`
	Config struct {
		AutoSize any    `json:"autosize"` // string or int
		Width    int    `json:"width"`
		Height   int    `json:"height"`
		Forward  int    `json:"forward"`
		CTime    int64  `json:"ctime"`
		Round    int    `json:"round"`
		Token    string `json:"token"`
		Type     string `json:"type"`
	} `json:"config"`
	// Extra struct {
	// 	AppType int   `json:"app_type"`
	// 	AppId   int64 `json:"appid"`
	// 	MsgSeq  int64 `json:"msg_seq"`
	// 	Uin     int64 `json:"uin"`
	// } `json:"extra"`
	Meta any `json:"meta"` // {
	// News struct {
	// 	Action         string `json:"action"`
	// 	AndroidPkgName string `json:"android_pkg_name"`
	// 	AppType        int    `json:"app_type"`
	// 	AppId          int64  `json:"appid"`
	// 	CTime          int64  `json:"ctime"`
	// 	Desc           string `json:"desc"`
	// 	JumpUrl        string `json:"jumpUrl"`
	// 	PreView        string `json:"preview"`
	// 	TagIcon        string `json:"tagIcon"`
	// 	SourceIcon     string `json:"source_icon"`
	// 	SourceUrl      string `json:"source_url"`
	// 	Tag            string `json:"tag"`
	// 	Title          string `json:"title"`
	// 	Uin            int64  `json:"uin"`
	// } `json:"news"`
	// Detail_1 struct {
	// 	AppId   string `json:"appid"`
	// 	AppType int    `json:"app_type"`
	// 	Title   string `json:"title"`
	// 	Desc    string `json:"desc"`
	// 	Icon    string `json:"icon"`
	// 	PreView string `json:"preview"`
	// 	Url     string `json:"url"`
	// 	Scene   int    `json:"scene"`
	// 	Host    struct {
	// 		Uin  int64  `json:"uin"`
	// 		Nick string `json:"nick"`
	// 	} `json:"host"`
	// } `json:"detail_1"`
	// Music struct {
	// 	Action         string `json:"action"`
	// 	AndroidPkgName string `json:"android_pkg_name"`
	// 	AppType        int    `json:"app_type"`
	// 	AppId          int64  `json:"appid"`
	// 	CTime          int64  `json:"ctime"`
	// 	Desc           string `json:"desc"`
	// 	JumpUrl        string `json:"jumpUrl"`
	// 	MusicUrl       string `json:"musicUrl"`
	// 	PreView        string `json:"preview"`
	// 	SourceMsgId    string `json:"source_msg_id"`
	// 	SourceIcon     string `json:"source_icon"`
	// 	SourceUrl      string `json:"source_url"`
	// 	Tag            string `json:"tag"`
	// 	Title          string `json:"title"`
	// 	Uin            int64  `json:"uin"`
	// } `json:"music"`
	// } `json:"meta"`
	Prompt string `json:"prompt"`
	Ver    string `json:"ver"`
	View   string `json:"view"`
}

type CardMessageMeta struct {
	Title string `json:"title"`
	Desc  string `json:"desc"`
	Tag   string `json:"tag"`
}

// 返回结构
type T struct {
	Status  string `json:"status"`
	RetCode int    `json:"retcode"`
	Message string `json:"message"`
	Wording string `json:"wording"`
	Data    any    `json:"data"`
}

// Window represents a fixed-window
type Window interface {
	// Start returns the start boundary
	Start() time.Time

	// Count returns the accumulated count
	Count() int64

	// AddCount increments the accumulated count by n
	AddCount(n int64)

	// Reset sets the state of the window with the given settings
	Reset(s time.Time, c int64)
}

type NewWindow func() Window

type LocalWindow struct {
	start int64
	count int64
}

type RateLimiter struct {
	size  time.Duration
	limit int64

	mu sync.Mutex

	curr Window
	prev Window
}

type QiDianAccountInfo struct {
	MasterUin  int64
	ExtName    string
	CreateTime int64

	bigDataReqAddrs   []string
	bigDataReqSession *bigDataSessionInfo
}

type handlerInfo struct {
	fun     func(i any, err error)
	dynamic bool
	params  network.RequestParams
}

func NewLocalWindow() *LocalWindow {
	return &LocalWindow{}
}

func (w *LocalWindow) Start() time.Time {
	return time.Unix(0, w.start)
}

func (w *LocalWindow) Count() int64 {
	return w.count
}

func (w *LocalWindow) AddCount(n int64) {
	w.count += n
}

func (w *LocalWindow) Reset(s time.Time, c int64) {
	w.start = s.UnixNano()
	w.count = c
}

func NewRateLimiter(size time.Duration, limit int64, newWindow NewWindow) *RateLimiter {
	currWin := newWindow()

	// The previous window is static (i.e. no add changes will happen within it),
	// so we always create it as an instance of LocalWindow
	prevWin := NewLocalWindow()

	return &RateLimiter{
		size:  size,
		limit: limit,
		curr:  currWin,
		prev:  prevWin,
	}

}

// Size returns the time duration of one window size
func (rl *RateLimiter) Size() time.Duration {
	return rl.size
}

// Limit returns the maximum events permitted to happen during one window size
func (rl *RateLimiter) Limit() int64 {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return rl.limit
}

func (rl *RateLimiter) SetLimit(limit int64) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.limit = limit
}

// shorthand for GrantN(time.Now(), 1)
func (rl *RateLimiter) Grant() bool {
	return rl.GrantN(time.Now(), 1)
}

// reports whether n events may happen at time now
func (rl *RateLimiter) GrantN(now time.Time, n int64) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	rl.advance(now)

	elapsed := now.Sub(rl.curr.Start())
	weight := float64(rl.size-elapsed) / float64(rl.size)
	count := int64(weight*float64(rl.prev.Count())) + rl.curr.Count()

	if count+n > rl.limit {
		return false
	}

	rl.curr.AddCount(n)
	return true
}

// advance updates the current/previous windows resulting from the passage of time
func (rl *RateLimiter) advance(now time.Time) {
	// Calculate the start boundary of the expected current-window.
	newCurrStart := now.Truncate(rl.size)

	diffSize := newCurrStart.Sub(rl.curr.Start()) / rl.size
	if diffSize >= 1 {
		// The current-window is at least one-window-size behind the expected one.
		newPrevCount := int64(0)
		if diffSize == 1 {
			// The new previous-window will overlap with the old current-window,
			// so it inherits the count.
			newPrevCount = rl.curr.Count()
		}

		rl.prev.Reset(newCurrStart.Add(-rl.size), newPrevCount)
		// The new current-window always has zero count.
		rl.curr.Reset(newCurrStart, 0)
	}
}

func (h *handlerInfo) getParams() network.RequestParams {
	if h == nil {
		return nil
	}
	return h.params
}

var decoders = map[string]func(*QQClient, *network.Packet) (any, error){
	"wtlogin.login":                                decodeLoginResponse,
	"wtlogin.exchange_emp":                         decodeExchangeEmpResponse,
	"wtlogin.trans_emp":                            decodeTransEmpResponse,
	"StatSvc.register":                             decodeClientRegisterResponse,
	"StatSvc.ReqMSFOffline":                        decodeMSFOfflinePacket,
	"MessageSvc.PushNotify":                        decodeSvcNotify,
	"OnlinePush.ReqPush":                           decodeOnlinePushReqPacket,
	"OnlinePush.PbPushTransMsg":                    decodeOnlinePushTransPacket,
	"OnlinePush.SidTicketExpired":                  decodeSidExpiredPacket,
	"ConfigPushSvc.PushReq":                        decodePushReqPacket,
	"MessageSvc.PbGetMsg":                          decodeMessageSvcPacket,
	"MessageSvc.PushForceOffline":                  decodeForceOfflinePacket,
	"PbMessageSvc.PbMsgWithDraw":                   decodeMsgWithDrawResponse,
	"friendlist.getFriendGroupList":                decodeFriendGroupListResponse,
	"friendlist.delFriend":                         decodeFriendDeleteResponse,
	"friendlist.GetTroopListReqV2":                 decodeGroupListResponse,
	"friendlist.GetTroopMemberListReq":             decodeGroupMemberListResponse,
	"group_member_card.get_group_member_card_info": decodeGroupMemberInfoResponse,
	"LongConn.OffPicUp":                            decodeOffPicUpResponse,
	"ProfileService.Pb.ReqSystemMsgNew.Group":      decodeSystemMsgGroupPacket,
	"ProfileService.Pb.ReqSystemMsgNew.Friend":     decodeSystemMsgFriendPacket,
	"OidbSvc.0xd79":                                decodeWordSegmentation,
	"OidbSvc.0x990":                                decodeTranslateResponse,
	"SummaryCard.ReqSummaryCard":                   decodeSummaryCardResponse,
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type SendMsg struct {
	GroupCode  int64
	Message    *message.SendingMessage
	NewStr     string
	ResultChan chan SendResp
}

var sendMessageQueue = make(chan SendMsg, 128)
var messageQueue = make(chan []byte, 128)
var md5Int64Mapping = make(map[int64]string)
var md5Int64MappingLock sync.Mutex

type DynamicInt64 int64

type MessageContent struct {
	Type string                 `json:"type"`
	Data map[string]interface{} `json:"data"`
}

type WebSocketMessage struct {
	PostType      string       `json:"post_type"`
	MetaEventType string       `json:"meta_event_type"`
	NoticeType    string       `json:"notice_type"`
	OperatorId    DynamicInt64 `json:"operator_id"`
	Status        struct {
		Online bool `json:"online"`
		Good   bool `json:"good"`
	} `json:"status"`
	RequestType string       `json:"request_type"`
	Comment     string       `json:"comment"`
	Flag        string       `json:"flag"`
	Interval    int          `json:"interval"`
	MessageType string       `json:"message_type"`
	Time        DynamicInt64 `json:"time"`
	SelfID      DynamicInt64 `json:"self_id"`
	SubType     string       `json:"sub_type"`
	Sender      struct {
		Age      int          `json:"age"`
		Card     string       `json:"card"`
		Nickname string       `json:"nickname"`
		Role     string       `json:"role"`
		UserID   DynamicInt64 `json:"user_id"`
	} `json:"sender"`
	UserID         DynamicInt64 `json:"user_id"`
	MessageID      DynamicInt64 `json:"message_id"`
	RealId         DynamicInt64 `json:"real_id"`
	GroupID        DynamicInt64 `json:"group_id"`
	MessageContent interface{}  `json:"message"`
	MessageSeq     DynamicInt64 `json:"message_seq"`
	RawMessage     string       `json:"raw_message"`
	TargetID       DynamicInt64 `json:"target_id"`
	SenderID       DynamicInt64 `json:"sender_id"`
	CardOld        string       `json:"card_old"`
	CardNew        string       `json:"card_new"`
	Duration       int32        `json:"duration"`
	Echo           string       `json:"echo,omitempty"`
}

func (d *DynamicInt64) UnmarshalJSON(b []byte) error {
	var n int64
	var s string

	// 首先尝试解析为字符串
	if err := json.Unmarshal(b, &s); err == nil {
		// 如果解析成功，再尝试将字符串解析为int64
		n, err = strconv.ParseInt(s, 10, 64)
		if err != nil { // 如果是非数字字符串
			// 进行 md5 处理
			hashed := md5.Sum([]byte(s))
			hexString := hex.EncodeToString(hashed[:])

			// 去除字母并取前15位
			numericPart := strings.Map(func(r rune) rune {
				if unicode.IsDigit(r) {
					return r
				}
				return -1
			}, hexString)

			if len(numericPart) < 15 {
				return fmt.Errorf("哈希过短: %s", numericPart)
			}

			n, err = strconv.ParseInt(numericPart[:15], 10, 64)
			if err != nil {
				return err
			}

			// 更新映射，但限制锁的范围
			md5Int64MappingLock.Lock()
			md5Int64Mapping[n] = s
			md5Int64MappingLock.Unlock()
		}
	} else {
		// 如果字符串解析失败，再尝试解析为int64
		if err := json.Unmarshal(b, &n); err != nil {
			return err
		}
	}

	*d = DynamicInt64(n)
	return nil
}

func (d DynamicInt64) ToInt64() int64 {
	return int64(d)
}

func (d DynamicInt64) ToString() string {
	return strconv.FormatInt(int64(d), 10)
}

func originalStringFromInt64(n int64) (string, bool) {
	md5Int64MappingLock.Lock()
	defer md5Int64MappingLock.Unlock()

	originalString, exists := md5Int64Mapping[n]
	return originalString, exists
}

// 假设 wsmsg.Message 是一个字符串，包含了形如 [CQ:at=xxxx] 的数据
func extractAtElements(str string) []*message.AtElement {
	re := regexp.MustCompile(`\[CQ:at=(\d+)\]`) // 正则表达式匹配[CQ:at=xxxx]其中xxxx是数字
	matches := re.FindAllStringSubmatch(str, -1)

	var elements []*message.AtElement
	for _, match := range matches {
		if len(match) == 2 {
			target, err := strconv.ParseInt(match[1], 10, 64) // 转化xxxx为int64
			if err != nil {
				continue // 如果转化失败，跳过此次循环
			}
			elements = append(elements, &message.AtElement{
				Target: target,
				// 如果有 Display 和 SubType 的信息，也可以在这里赋值
			})
		}
	}
	return elements
}

func parseEcho(echo string) (string, string) {
	parts := strings.SplitN(echo, ":", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return echo, ""
}

func (c *QQClient) handleConnection(ws *websocket.Conn) {
	//处理ws消息并控制重连
	defer ws.Close()
	for {
		_, p, err := ws.ReadMessage()
		if err != nil {
			logger.Error(err)
			return
		}
		// 储存收到的消息
		messageQueue <- p
	}
}

func (c *QQClient) processMessage() {
	for {
		select {
		case p := <-messageQueue:
			go c.handleResponse(p)
		}
	}
}

func (c *QQClient) handleResponse(p []byte) {
	go logger.Tracef("Received message: %s", string(p))
	//初步解析
	var basicMsg BasicMessage
	err := json.Unmarshal(p, &basicMsg)
	if err != nil {
		//log.Println("Failed to parse basic message:", err)
		logger.Errorf("Failed to parse basic message: %v", err)
		return
	}
	if basicMsg.Echo != "" {
		respCh, isResponse := c.responseCh[basicMsg.Echo]
		if !isResponse {
			logger.Warnf("No response channel for echo: %s", basicMsg.Echo)
			return
		}
		var resp T
		if err = json.Unmarshal(p, &resp); err != nil {
			logger.Errorf("Failed to unmarshal get message response: %v", err)
			return
		}
		respCh <- &resp
		// action, _ := parseEcho(basicMsg.Echo)
		// logger.Debugf("Received action: %s", action)
		//根据echo判断
		// switch action {
		// case "get_group_list":
		// 	respCh, isResponse := c.responseChans[basicMsg.Echo]
		// 	if !isResponse {
		// 		logger.Warnf("No response channel for group list: %s", basicMsg.Echo)
		// 		return
		// 	}
		// 	var groupData ResponseGroupsData
		// 	if err := json.Unmarshal(p, &groupData); err != nil {
		// 		//log.Println("Failed to unmarshal group data:", err)
		// 		logger.Errorf("Failed to unmarshal group data: %v", err)
		// 		return
		// 	}
		// 	respCh <- &groupData
		// case "get_group_info":
		// 	respCh, isResponse := c.responseGroupChans[basicMsg.Echo]
		// 	if !isResponse {
		// 		logger.Warnf("No response channel for group info: %s", basicMsg.Echo)
		// 		return
		// 	}
		// 	var groupData ResponseGroupData
		// 	if err := json.Unmarshal(p, &groupData); err != nil {
		// 		logger.Errorf("Failed to unmarshal group data: %v", err)
		// 		return
		// 	}
		// 	respCh <- &groupData
		// case "get_group_member_list":
		// 	respCh, isResponse := c.responseMembers[basicMsg.Echo]
		// 	if !isResponse {
		// 		logger.Warnf("No response channel for group member list: %s", basicMsg.Echo)
		// 		return
		// 	}
		// 	var memberData ResponseGroupMemberData
		// 	if err := json.Unmarshal(p, &memberData); err != nil {
		// 		//log.Println("Failed to unmarshal group member data:", err)
		// 		logger.Errorf("Failed to unmarshal group member data: %v", err)
		// 		return
		// 	}
		// 	respCh <- &memberData
		// case "get_friend_list":
		// 	respCh, isResponse := c.responseFriends[basicMsg.Echo]
		// 	if !isResponse {
		// 		logger.Warnf("No response channel for friend list: %s", basicMsg.Echo)
		// 		return
		// 	}
		// 	var friendData ResponseFriendList
		// 	if err := json.Unmarshal(p, &friendData); err != nil {
		// 		//log.Println("Failed to unmarshal friend data:", err)
		// 		logger.Errorf("Failed to unmarshal friend data: %v", err)
		// 		return
		// 	}
		// 	respCh <- &friendData
		// case "send_group_msg", "send_private_msg":
		// 	respCh, isResponse := c.responseMessage[basicMsg.Echo]
		// 	if !isResponse {
		// 		logger.Warnf("No response channel for message: %s", basicMsg.Echo)
		// 		return
		// 	}
		// 	var SendResp ResponseSendMessage
		// 	if err := json.Unmarshal(p, &SendResp); err != nil {
		// 		//log.Println("Failed to unmarshal send message response:", err)
		// 		logger.Errorf("Failed to unmarshal send message response: %v", err)
		// 		return
		// 	}
		// 	respCh <- &SendResp
		// case "set_group_leave":
		// 	respCh, isResponse := c.responseSetLeave[basicMsg.Echo]
		// 	if !isResponse {
		// 		logger.Warnf("No response channel for set group leave: %s", basicMsg.Echo)
		// 		return
		// 	}
		// 	var SendResp ResponseSetGroupLeave
		// 	if err = json.Unmarshal(p, &SendResp); err != nil {
		// 		logger.Errorf("Failed to unmarshal set group leave response: %v", err)
		// 		return
		// 	}
		// 	respCh <- &SendResp
		// case "get_stranger_info":
		// 	respCh, isResponse := c.responseStrangerInfo[basicMsg.Echo]
		// 	if !isResponse {
		// 		logger.Warnf("No response channel for get stranger info: %s", basicMsg.Echo)
		// 		return
		// 	}
		// 	var SendResp ResponseGetStrangerInfo
		// 	if err = json.Unmarshal(p, &SendResp); err != nil {
		// 		logger.Errorf("Failed to unmarshal get stranger info response: %v", err)
		// 		return
		// 	}
		// 	respCh <- &SendResp
		// case "set_friend_add_request":
		// 	logger.Debug("Received set friend add request")
		// case "set_group_add_request":
		// 	logger.Debug("Received set group add request")
		// case "get_group_member_info":
		// 	respCh, isResponse := c.responseCh[basicMsg.Echo]
		// 	if !isResponse {
		// 		logger.Warnf("No response channel for get group member info: %s", basicMsg.Echo)
		// 		return
		// 	}
		// 	var resp T
		// 	if err = json.Unmarshal(p, &resp); err != nil {
		// 		logger.Errorf("Failed to unmarshal get group member info response: %v", err)
		// 		return
		// 	}
		// 	respCh <- &resp
		// default:
		// 	logger.Warnf("Unknown action: %s", action)
		// }
		//其他类型
		// delete(c.responseChans, basicMsg.Echo)
	} else {
		// 解析消息
		var wsmsg WebSocketMessage
		err = json.Unmarshal(p, &wsmsg)
		if err != nil {
			//log.Println("Failed to parse message:", err)
			logger.Errorf("Failed to parse message: %v", err)
			return
		}
		// 存储 echo
		// c.currentEcho = wsmsg.Echo
		c.handleMessage(wsmsg)
	}
}

func (c *QQClient) handleMessage(wsmsg WebSocketMessage) {
	// 处理解析后的消息
	if wsmsg.MessageType == "group" || wsmsg.MessageType == "private" {
		if wsmsg.MessageType == "group" {
			wsMsg := &message.GroupMessage{
				Id:        int32(wsmsg.MessageID),
				GroupCode: wsmsg.GroupID.ToInt64(),
				GroupName: "",
				// GroupName: groupName,
				Sender: &message.Sender{
					Uin:      wsmsg.Sender.UserID.ToInt64(),
					Nickname: wsmsg.Sender.Nickname,
					CardName: wsmsg.Sender.Card,
					IsFriend: false,
				},
				Time:           int32(wsmsg.Time),
				OriginalObject: nil,
			}
			c.ChatMsgHandler(wsmsg, wsMsg, nil)
		} else if wsmsg.MessageType == "private" {
			wsMsg := &message.PrivateMessage{
				Id:         int32(wsmsg.MessageID),
				InternalId: 0,
				Self:       c.Uin,
				Target:     wsmsg.Sender.UserID.ToInt64(),
				Time:       int32(wsmsg.Time),
				Sender: &message.Sender{
					Uin:      wsmsg.Sender.UserID.ToInt64(),
					Nickname: wsmsg.Sender.Nickname,
					CardName: "", // Private message might not have a Card
					IsFriend: wsmsg.SubType == "friend",
				},
			}
			// 拿到发送自身发送的消息时修改为正确目标
			if wsmsg.PostType == "message_sent" {
				wsMsg.Target = int64(wsmsg.TargetID)
			}
			c.ChatMsgHandler(wsmsg, nil, wsMsg)
		}
		// if MessageContent, ok := wsmsg.MessageContent.(string); ok {
		// 	// 替换字符串中的"\/"为"/"
		// 	MessageContent = strings.Replace(MessageContent, "\\/", "/", -1)
		// 	// 使用extractAtElements函数从wsmsg.Message中提取At元素
		// 	atElements := extractAtElements(MessageContent)
		// 	// 将提取的At元素和文本元素都添加到g.Elements
		// 	pMsg.Elements = append(pMsg.Elements, &message.TextElement{Content: MessageContent})
		// 	for _, elem := range atElements {
		// 		pMsg.Elements = append(pMsg.Elements, elem)
		// 	}
		// } else if contentArray, ok := wsmsg.MessageContent.([]interface{}); ok {
		// 	for _, contentInterface := range contentArray {
		// 		contentMap, ok := contentInterface.(map[string]interface{})
		// 		if !ok {
		// 			continue
		// 		}

		// 		contentType, ok := contentMap["type"].(string)
		// 		if !ok {
		// 			continue
		// 		}

		// 		switch contentType {
		// 		case "text":
		// 			text, ok := contentMap["data"].(map[string]interface{})["text"].(string)
		// 			if ok {
		// 				// 替换字符串中的"\/"为"/"
		// 				text = strings.Replace(text, "\\/", "/", -1)
		// 				pMsg.Elements = append(pMsg.Elements, &message.TextElement{Content: text})
		// 			}
		// 		case "at":
		// 			if data, ok := contentMap["data"].(map[string]interface{}); ok {
		// 				var qq int64
		// 				if qqData, ok := data["qq"].(string); ok {
		// 					if qqData != "all" {
		// 						qq, err = strconv.ParseInt(qqData, 10, 64)
		// 						if err != nil {
		// 							logger.Errorf("Failed to parse qq: %v", err)
		// 							continue
		// 						}
		// 					} else {
		// 						qq = 0
		// 					}
		// 				} else if qqData, ok := data["qq"].(int64); ok {
		// 					qq = qqData
		// 				}
		// 				//atText := fmt.Sprintf("[CQ:at,qq=%s]", qq)
		// 				pMsg.Elements = append(pMsg.Elements, &message.AtElement{Target: int64(qq), Display: pMsg.Sender.DisplayName()})
		// 			}
		// 		case "face":
		// 			faceID := 0
		// 			if face, ok := contentMap["data"].(map[string]interface{})["id"].(string); ok {
		// 				faceID, _ = strconv.Atoi(face)
		// 			} else if face, ok := contentMap["data"].(map[string]interface{})["id"].(float64); ok {
		// 				faceID = int(face)
		// 			}
		// 			pMsg.Elements = append(pMsg.Elements, &message.FaceElement{Index: int32(faceID)})
		// 		case "mface":
		// 			if mface, ok := contentMap["data"].(map[string]interface{}); ok {
		// 				tabId, _ := strconv.Atoi(mface["emoji_package_id"].(string))
		// 				pMsg.Elements = append(pMsg.Elements, &message.MarketFaceElement{
		// 					Name:       mface["summary"].(string),
		// 					FaceId:     []byte(mface["emoji_id"].(string)),
		// 					TabId:      int32(tabId),
		// 					MediaType:  2,
		// 					EncryptKey: []byte(mface["key"].(string)),
		// 				})
		// 			}
		// 		case "image":
		// 			image, ok := contentMap["data"].(map[string]interface{})
		// 			if ok {
		// 				size := 0
		// 				if tmp, ok := image["file_size"].(string); ok {
		// 					size, err = strconv.Atoi(tmp)
		// 				}
		// 				if contentMap["subType"] == nil {
		// 					pMsg.Elements = append(pMsg.Elements, &message.FriendImageElement{
		// 						Size: int32(size),
		// 						Url:  image["url"].(string),
		// 					})
		// 				} else {
		// 					if contentMap["subType"].(float64) == 1.0 {
		// 						pMsg.Elements = append(pMsg.Elements, &message.MarketFaceElement{
		// 							Name:       "[图片表情]",
		// 							FaceId:     []byte(image["file"].(string)),
		// 							SubType:    3,
		// 							TabId:      0,
		// 							MediaType:  2,
		// 							EncryptKey: []byte("0"),
		// 						})
		// 					} else {
		// 						pMsg.Elements = append(pMsg.Elements, &message.FriendImageElement{
		// 							Size: int32(size),
		// 							Url:  image["url"].(string),
		// 						})
		// 					}
		// 				}
		// 			}
		// 		case "reply":
		// 			replySeq := 0
		// 			if replyID, ok := contentMap["data"].(map[string]interface{})["id"].(string); ok {
		// 				replySeq, _ = strconv.Atoi(replyID)
		// 			} else if replyID, ok := contentMap["data"].(map[string]interface{})["id"].(float64); ok {
		// 				replySeq = int(replyID)
		// 			}
		// 			pMsg.Elements = append(pMsg.Elements, &message.ReplyElement{
		// 				ReplySeq: int32(replySeq),
		// 				Sender:   pMsg.Sender.Uin,
		// 				Time:     pMsg.Time,
		// 				Elements: pMsg.Elements,
		// 			})
		// 		case "record":
		// 			record, ok := contentMap["data"].(map[string]interface{})
		// 			if ok {
		// 				fileSize := 0
		// 				filePath := ""
		// 				if size, ok := record["file_size"].(string); ok {
		// 					fileSize, _ = strconv.Atoi(size)
		// 				}
		// 				if path, ok := record["path"].(string); ok {
		// 					filePath = path
		// 				}
		// 				pMsg.Elements = append(pMsg.Elements, &message.VoiceElement{
		// 					Name: record["file"].(string),
		// 					Url:  filePath,
		// 					Size: int32(fileSize),
		// 				})
		// 			}
		// 		case "json":
		// 			// 处理json消息
		// 			if card, ok := contentMap["data"].(map[string]interface{}); ok {
		// 				switch card["data"].(type) {
		// 				case string:
		// 					var j CardMessage
		// 					err := json.Unmarshal([]byte(card["data"].(string)), &j)
		// 					if err != nil {
		// 						logger.Errorf("Failed to parse card message: %v", err)
		// 						continue
		// 					}
		// 					tag, title, desc, text := "", "", "", ""
		// 					if j.Meta.News.Title != "" {
		// 						tag = j.Meta.News.Tag
		// 						title = j.Meta.News.Title
		// 						desc = j.Meta.News.Desc
		// 						text = "[卡片][" + tag + "][" + title + "]" + desc
		// 					} else if j.Meta.Detail_1.Title != "" {
		// 						tag = j.Meta.Detail_1.Title
		// 						desc = j.Meta.Detail_1.Desc
		// 						text = "[卡片][" + tag + "]" + desc
		// 					}
		// 					pMsg.Elements = append(pMsg.Elements, &message.LightAppElement{Content: text})
		// 				default:
		// 					logger.Errorf("Unknown card message type: %v", card)
		// 				}
		// 			}
		// 		case "file":
		// 			_, ok := contentMap["data"].(map[string]interface{})
		// 			if ok {
		// 				text := "[文件]" // + file["file"].(string)
		// 				pMsg.Elements = append(pMsg.Elements, &message.TextElement{Content: text})
		// 			}
		// 		case "forward":
		// 			forward, ok := contentMap["data"].(map[string]interface{})
		// 			if ok {
		// 				text := "[合并转发]" // + forward["id"].(string)
		// 				pMsg.Elements = append(pMsg.Elements, &message.ForwardElement{
		// 					Content: text,
		// 					ResId:   forward["id"].(string),
		// 				})
		// 			}
		// 		case "video":
		// 			video, ok := contentMap["data"].(map[string]interface{})
		// 			if ok {
		// 				fileSize, _ := strconv.Atoi(video["file_size"].(string))
		// 				pMsg.Elements = append(pMsg.Elements, &message.ShortVideoElement{
		// 					Name: video["file"].(string),
		// 					Uuid: []byte(video["file_id"].(string)),
		// 					Size: int32(fileSize),
		// 					Url:  video["url"].(string),
		// 				})
		// 				/*
		// 					LLOB收到的video类型数据示例：
		// 					map[string]interface {} ["file": ".mp4", "path": "C:\\Users\\adevi\\Documents\\Tencent Files\\1143469507\\nt_qq\\nt_data\\Video\\2024-07\\Ori\\b24e3a5e725ef156c6c1a2952fba7e95.mp4", "file_id": "CgoxMTQzNDY5NTA3EhRVzq7teOgsGiZb6ie79Cs2KXFkyRjZqP8EIIcLKJ-p0cmpiocDUID1JA", "file_size": "10474585", "url": "https://multimedia.nt.qq.com.cn:443/download?appid=1415&format=origin&orgfmt=t264&spec=0&client_proto=ntv2&client_appid=537226356&client_type=win&client_ver=9.9.11-24402&client_down_type=auto&client_aio_type=aio&rkey=CAQSgAEAeiVhDYuSzI7DqvUpMmuP90fvCJi_a6tITce7D-Rn8ay-MG4uJUD8kfUH9LuBi10T2af2g9t4hYhLT_uunXuLZ7kiw7wEebeJfsVdTnULb-ouAXvpoj7_5N1acir0NOjJ4jtOdM9UPXd-2h93k9On49iHC1QdMk4vMg4aU6DsUQ", ]
		// 				*/
		// 			}
		// 		case "markdown":
		// 			text := "[Markdown]"
		// 			pMsg.Elements = append(pMsg.Elements, &message.TextElement{Content: text})
		// 		default:
		// 			logger.Warnf("Unknown content type: %s", contentType)
		// 		}
		// 	}
		// }
		// selfIDStr := strconv.FormatInt(int64(wsmsg.SelfID), 10)
		// if selfIDStr == strconv.FormatInt(int64(wsmsg.Sender.UserID), 10) {
		// 	//logger.Debugf("准备c.SelfPrivateMessageEvent.dispatch(c, pMsg)")
		// 	c.SelfPrivateMessageEvent.dispatch(c, pMsg)
		// } else {
		// 	//logger.Debugf("准备c.PrivateMessageEvent.dispatch(c, pMsg)")
		// 	c.PrivateMessageEvent.dispatch(c, pMsg)
		// }
		// c.OutputReceivingMessage(pMsg)
	} else if wsmsg.MessageType == "" {
		if wsmsg.PostType == "meta_event" {
			logger.Debugf("收到 元事件 消息：%s: %s", wsmsg.SubType, wsmsg.MetaEventType)
			// 元事件
			if wsmsg.MetaEventType == "lifecycle" {
				// 刷新BOT状态
				c.Uin = int64(wsmsg.SelfID)
				c.Online.Store(true)
				c.alive = true
			} else if wsmsg.MetaEventType == "heartbeat" {
				c.Online.Store(wsmsg.Status.Online)
				c.alive = wsmsg.Status.Good
			}
		} else if wsmsg.PostType == "notice" {
			const memberNotFind = "Failed to get group member info: %v"
			logger.Infof("收到 通知事件 消息：%s: %s", wsmsg.NoticeType, wsmsg.SubType)
			// 通知事件
			sync := false
			if wsmsg.NoticeType == "group_admin" {
				// 如果是权限变更，则仅刷新群成员列表
				sync = true
			} else if wsmsg.NoticeType == "group_increase" {
				// 根据是否与自身有关来选择性刷新群列表
				sync = true
				if wsmsg.UserID.ToInt64() == c.Uin {
					ret, err := c.GetGroupInfo(wsmsg.GroupID.ToInt64())
					if err == nil {
						skip := false
						for i, group := range c.GroupList {
							if group.Code == wsmsg.GroupID.ToInt64() {
								c.GroupList[i] = ret
								skip = true
								logger.Debugf("Group %d updated", wsmsg.GroupID.ToInt64())
								break
							}
						}
						if !skip {
							logger.Debugf("Group %d not found in GroupList, adding it", wsmsg.GroupID.ToInt64())
							c.GroupList = append(c.GroupList, ret)
						}
					} else {
						logger.Warnf(memberNotFind, err)
					}
				} else {
					member, err := c.GetGroupMemberInfo(wsmsg.GroupID.ToInt64(), wsmsg.UserID.ToInt64())
					if err == nil {
						c.GroupMemberJoinEvent.dispatch(c, &MemberJoinGroupEvent{
							Group:  member.Group,
							Member: member,
						})
					} else {
						logger.Warnf(memberNotFind, err)
					}
				}
			} else if wsmsg.NoticeType == "group_decrease" {
				// 自己被踢
				if wsmsg.SubType == "kick_me" {
					group := c.FindGroup(wsmsg.GroupID.ToInt64())
					operator := group.FindMember(wsmsg.OperatorId.ToInt64())
					if operator != nil {
						c.GroupLeaveEvent.dispatch(c, &GroupLeaveEvent{
							Group:    group,
							Operator: operator,
						})
					} else {
						logger.Warnf(memberNotFind, wsmsg.OperatorId)
					}
					// for i, group := range c.GroupList {
					// 	if group.Code == wsmsg.GroupID.ToInt64() {
					// 		c.GroupList = append(c.GroupList[:i], c.GroupList[i+1:]...)
					// 		logger.Debugf("Group %d removed from GroupList", wsmsg.GroupID.ToInt64())
					// 		break
					// 	}
					// }
				} else {
					// 其他人退群/被踢
					sync = true
					member := c.FindGroup(wsmsg.GroupID.ToInt64()).FindMember(wsmsg.UserID.ToInt64())
					if member != nil {
						operator := member
						if wsmsg.OperatorId != wsmsg.UserID {
							operator = c.FindGroup(wsmsg.GroupID.ToInt64()).FindMember(wsmsg.OperatorId.ToInt64())
						}
						if operator != nil {
							c.GroupMemberLeaveEvent.dispatch(c, &MemberLeaveGroupEvent{
								Group:    member.Group,
								Member:   member,
								Operator: operator,
							})
						} else {
							logger.Warnf(memberNotFind, wsmsg.OperatorId)
						}
					} else {
						logger.Warnf(memberNotFind, wsmsg.UserID)
					}
				}
			} else if wsmsg.NoticeType == "friend_add" {
				// 新增好友事件
				friend, err := c.GetStrangerInfo(wsmsg.UserID.ToInt64())
				if err == nil {
					c.NewFriendEvent.dispatch(c, &NewFriendEvent{
						Friend: &FriendInfo{
							Uin:      wsmsg.UserID.ToInt64(),
							Nickname: friend.Nick,
							Remark:   friend.Remark,
							FaceId:   0,
						},
					})
				} else {
					c.ReloadFriendList()
				}
				// c.SyncTickerControl(2, WebSocketMessage{}, false)
			} else if wsmsg.SubType == "poke" {
				if wsmsg.GroupID != 0 {
					// 群戳一戳事件
				} else {
					// 好友戳一戳事件
					if wsmsg.TargetID.ToInt64() == c.Uin {
						c.FriendNotifyEvent.dispatch(c, &FriendPokeNotifyEvent{
							Sender:   wsmsg.SenderID.ToInt64(),
							Receiver: wsmsg.TargetID.ToInt64(),
						})
					}
				}
				// 戳一戳事件（返回示例）
				// {
				// 	"time": 1719850095,
				// 	"self_id": 313575803,
				// 	"post_type": "notice",
				// 	"notice_type": "notify",
				// 	"sub_type": "poke",
				// 	"target_id": 313575803,
				// 	"user_id": 1143469507,
				// 	"sender_id": 1143469507 // 或 "group_id"
				// }
			} else if wsmsg.NoticeType == "group_card" {
				member, err := c.GetGroupMemberInfo(wsmsg.GroupID.ToInt64(), wsmsg.UserID.ToInt64())
				if err == nil {
					c.MemberCardUpdatedEvent.dispatch(c, &MemberCardUpdatedEvent{
						Group:   member.Group,
						OldCard: wsmsg.CardOld,
						Member:  member,
					})
				} else {
					logger.Warnf(memberNotFind, err)
				}
			} else if wsmsg.NoticeType == "group_ban" {
				skip := false
				var member *GroupMemberInfo
				if wsmsg.UserID != 0 {
					member = c.FindGroup(wsmsg.GroupID.ToInt64()).FindMember(wsmsg.UserID.ToInt64())
				} else {
					member = c.FindGroup(wsmsg.GroupID.ToInt64()).FindMember(c.Uin)
					if member.Permission == Member {
						wsmsg.UserID = DynamicInt64(c.Uin)
					} else {
						skip = true
					}
				}
				if !skip {
					if wsmsg.Duration > 0 {
						member.ShutUpTimestamp = time.Now().Add(time.Second * time.Duration(wsmsg.Duration)).Unix()
					} else {
						member.ShutUpTimestamp = 0
					}
					if wsmsg.Duration == -1 {
						wsmsg.Duration = 268435455
					}
					c.GroupMuteEvent.dispatch(c, &GroupMuteEvent{
						GroupCode:   wsmsg.GroupID.ToInt64(),
						OperatorUin: wsmsg.OperatorId.ToInt64(),
						TargetUin:   wsmsg.UserID.ToInt64(),
						Time:        wsmsg.Duration,
					})
				}
			}
			// 选择性执行上述结果
			if sync {
				if c.GroupList != nil {
					c.SyncGroupMembers(wsmsg.GroupID)
					// c.SyncTickerControl(1, wsmsg, refresh)
				}
			}
		} else if wsmsg.PostType == "request" {
			const StragngerInfoErr = "Failed to get stranger info: %v"
			logger.Infof("收到 请求事件 消息: %s: %s", wsmsg.RequestType, wsmsg.SubType)
			// 请求事件
			if wsmsg.RequestType == "friend" {
				friendRequest := NewFriendRequest{
					RequestId:     time.Now().UnixNano() / 1e6,
					Message:       wsmsg.Comment,
					RequesterUin:  wsmsg.UserID.ToInt64(),
					RequesterNick: "陌生人",
					Flag:          wsmsg.Flag,
					client:        c,
				}
				info, err := c.GetStrangerInfo(friendRequest.RequesterUin)
				if err == nil {
					friendRequest.RequesterNick = info.Nick
				} else {
					logger.Warnf(StragngerInfoErr, err)
				}
				// 好友申请
				c.NewFriendRequestEvent.dispatch(c, &friendRequest)
			}
			if wsmsg.RequestType == "group" {
				if wsmsg.SubType == "add" {
					// 加群申请
					groupRequest := UserJoinGroupRequest{
						RequestId:     time.Now().UnixNano() / 1e6,
						Message:       wsmsg.Comment,
						RequesterUin:  wsmsg.UserID.ToInt64(),
						RequesterNick: "陌生人",
						GroupCode:     wsmsg.GroupID.ToInt64(),
						GroupName:     "未知",
						Flag:          wsmsg.Flag,
						client:        c,
					}
					user, err := c.GetStrangerInfo(groupRequest.RequesterUin)
					if err == nil {
						groupRequest.RequesterNick = user.Nick
					} else {
						logger.Warnf(StragngerInfoErr, err)
					}
					groupInfo := c.FindGroupByUin(groupRequest.GroupCode)
					if groupInfo != nil {
						groupRequest.GroupName = groupInfo.Name
					}
					c.UserWantJoinGroupEvent.dispatch(c, &groupRequest)
				} else if wsmsg.SubType == "invite" {
					// 群邀请
					groupRequest := GroupInvitedRequest{
						RequestId:   time.Now().UnixNano() / 1e6,
						InvitorUin:  wsmsg.UserID.ToInt64(),
						InvitorNick: "陌生人",
						GroupCode:   wsmsg.GroupID.ToInt64(),
						GroupName:   "未知",
						Flag:        wsmsg.Flag,
						client:      c,
					}
					user, err := c.GetStrangerInfo(groupRequest.InvitorUin)
					if err == nil {
						groupRequest.InvitorNick = user.Nick
					} else {
						logger.Warnf(StragngerInfoErr, err)
					}
					groupInfo := c.FindGroupByUin(groupRequest.GroupCode)
					if groupInfo != nil {
						groupRequest.GroupName = groupInfo.Name
					}
					c.GroupInvitedEvent.dispatch(c, &groupRequest)
				}
			}
		}
	}
}

func (c *QQClient) ChatMsgHandler(wsmsg WebSocketMessage, g *message.GroupMessage, pMsg *message.PrivateMessage) {
	isGroupMsg := true
	var err error
	if wsmsg.MessageType == "private" {
		isGroupMsg = false
	}
	if MessageContent, ok := wsmsg.MessageContent.(string); ok {
		// 替换字符串中的"\/"为"/"
		MessageContent = strings.Replace(MessageContent, "\\/", "/", -1)
		// 使用extractAtElements函数从wsmsg.Message中提取At元素
		atElements := extractAtElements(MessageContent)
		// 将提取的At元素和文本元素都添加到g.Elements
		if isGroupMsg {
			g.Elements = append(g.Elements, &message.TextElement{Content: MessageContent})
			for _, elem := range atElements {
				g.Elements = append(g.Elements, elem)
			}
		} else {
			pMsg.Elements = append(pMsg.Elements, &message.TextElement{Content: MessageContent})
			for _, elem := range atElements {
				pMsg.Elements = append(pMsg.Elements, elem)
			}
		}
	} else if contentArray, ok := wsmsg.MessageContent.([]interface{}); ok {
		for _, contentInterface := range contentArray {
			contentMap, ok := contentInterface.(map[string]interface{})
			if !ok {
				continue
			}
			contentType, ok := contentMap["type"].(string)
			if !ok {
				continue
			}
			switch contentType {
			case "text":
				text, ok := contentMap["data"].(map[string]interface{})["text"].(string)
				if ok {
					// 替换字符串中的"\/"为"/"
					text = strings.Replace(text, "\\/", "/", -1)
					if isGroupMsg {
						g.Elements = append(g.Elements, &message.TextElement{Content: text})
					} else {
						pMsg.Elements = append(pMsg.Elements, &message.TextElement{Content: text})
					}
				}
			case "at":
				if data, ok := contentMap["data"].(map[string]interface{}); ok {
					var qq int64
					if qqData, ok := data["qq"].(string); ok {
						if qqData != "all" {
							qq, err = strconv.ParseInt(qqData, 10, 64)
							if err != nil {
								logger.Errorf("Failed to parse qq: %v", err)
								continue
							}
						} else {
							qq = 0
						}
					} else if qqData, ok := data["qq"].(int64); ok {
						qq = qqData
					}
					if isGroupMsg {
						g.Elements = append(g.Elements, &message.AtElement{Target: qq, Display: g.Sender.DisplayName()})
					} else {
						pMsg.Elements = append(pMsg.Elements, &message.AtElement{Target: qq, Display: pMsg.Sender.DisplayName()})
					}
				}
			case "face":
				faceID := 0
				if face, ok := contentMap["data"].(map[string]interface{})["id"].(string); ok {
					faceID, _ = strconv.Atoi(face)
				} else if face, ok := contentMap["data"].(map[string]interface{})["id"].(float64); ok {
					faceID = int(face)
				}
				if isGroupMsg {
					g.Elements = append(g.Elements, &message.FaceElement{Index: int32(faceID)})
				} else {
					pMsg.Elements = append(pMsg.Elements, &message.FaceElement{Index: int32(faceID)})
				}
			case "mface":
				if mface, ok := contentMap["data"].(map[string]interface{}); ok {
					tabId, _ := strconv.Atoi(mface["emoji_package_id"].(string))
					msg := &message.MarketFaceElement{
						Name:       "",
						FaceId:     []byte(mface["emoji_id"].(string)),
						TabId:      int32(tabId),
						MediaType:  2,
						EncryptKey: []byte(mface["key"].(string)),
					}
					if summary, ok := mface["summary"].(string); ok {
						msg.Name = summary
					}
					if isGroupMsg {
						g.Elements = append(g.Elements, msg)
					} else {
						pMsg.Elements = append(pMsg.Elements, msg)
					}
				}
			case "image":
				image, ok := contentMap["data"].(map[string]interface{})
				if ok {
					size := 0
					if tmp, ok := image["file_size"].(string); ok {
						size, err = strconv.Atoi(tmp)
					}
					if contentMap["subType"] == nil {
						if isGroupMsg {
							msg := &message.GroupImageElement{
								Size: int32(size),
								Url:  image["url"].(string),
							}
							g.Elements = append(g.Elements, msg)
						} else {
							msg := &message.FriendImageElement{
								Size: int32(size),
								Url:  image["url"].(string),
							}
							pMsg.Elements = append(pMsg.Elements, msg)
						}
					} else {
						if contentMap["subType"].(float64) == 1.0 {
							msg := &message.MarketFaceElement{
								Name:       "[图片表情]",
								FaceId:     []byte(image["file"].(string)),
								SubType:    3,
								TabId:      0,
								MediaType:  2,
								EncryptKey: []byte("0"),
							}
							if isGroupMsg {
								g.Elements = append(g.Elements, msg)
							} else {
								pMsg.Elements = append(pMsg.Elements, msg)
							}
						} else {
							if isGroupMsg {
								msg := &message.GroupImageElement{
									Size: int32(size),
									Url:  image["url"].(string),
								}
								g.Elements = append(g.Elements, msg)
							} else {
								msg := &message.FriendImageElement{
									Size: int32(size),
									Url:  image["url"].(string),
								}
								pMsg.Elements = append(pMsg.Elements, msg)
							}
						}
					}
				}
			case "reply":
				replySeq := 0
				if replyID, ok := contentMap["data"].(map[string]interface{})["id"].(string); ok {
					replySeq, _ = strconv.Atoi(replyID)
				} else if replyID, ok := contentMap["data"].(map[string]interface{})["id"].(float64); ok {
					replySeq = int(replyID)
				}

				if isGroupMsg {
					msg := &message.ReplyElement{
						ReplySeq: int32(replySeq),
						Sender:   g.Sender.Uin,
						GroupID:  g.GroupCode,
						Time:     g.Time,
						Elements: g.Elements,
					}
					g.Elements = append(g.Elements, msg)
				} else {
					msg := &message.ReplyElement{
						ReplySeq: int32(replySeq),
						Sender:   pMsg.Sender.Uin,
						Time:     pMsg.Time,
						Elements: pMsg.Elements,
					}
					pMsg.Elements = append(pMsg.Elements, msg)
				}
			case "record":
				record, ok := contentMap["data"].(map[string]interface{})
				if ok {
					fileSize := 0
					filePath := ""
					if size, ok := record["file_size"].(string); ok {
						fileSize, _ = strconv.Atoi(size)
					}
					if path, ok := record["path"].(string); ok {
						filePath = path
					}
					msg := &message.VoiceElement{
						Name: record["file"].(string),
						Url:  filePath,
						Size: int32(fileSize),
					}
					if isGroupMsg {
						g.Elements = append(g.Elements, msg)
					} else {
						pMsg.Elements = append(pMsg.Elements, msg)
					}
				}
			case "json":
				// 处理json消息
				if card, ok := contentMap["data"].(map[string]interface{}); ok {
					switch card["data"].(type) {
					case string:
						var j CardMessage
						err := json.Unmarshal([]byte(card["data"].(string)), &j)
						if err != nil {
							logger.Errorf("Failed to parse card message: %v", err)
							continue
						}
						needDec := false
						tag, title, desc, text := "", "", "", "[卡片]["
						if meta, ok := j.Meta.(map[string]interface{}); ok {
							var metaData any
							var metaMap = CardMessageMeta{
								Title: "未知",
								Desc:  "未知",
								Tag:   "未知",
							}
							if metaData, ok = meta["news"].(map[string]interface{}); ok {
								needDec = true
							} else if metaData, ok = meta["music"].(map[string]interface{}); ok {
								needDec = true
							} else if metaData, ok = meta["detail_1"].(map[string]interface{}); ok {
								needDec = true
								if host, ok := metaData.(map[string]interface{})["host"].(map[string]interface{}); ok {
									metaMap.Tag = host["nick"].(string)
								}
							} else if metaData, ok = meta["contact"].(map[string]interface{}); ok {
								needDec = true
							} else if metaData, ok = meta["video"].(map[string]interface{}); ok {
								metaMap = CardMessageMeta{
									Title: metaData.(map[string]interface{})["nickname"].(string),
									Desc:  metaData.(map[string]interface{})["title"].(string),
									Tag:   "视频",
								}
							} else if metaData, ok = meta["detail"].(map[string]interface{}); ok {
								if channel, ok := metaData.(map[string]interface{})["channel_info"].(map[string]interface{}); ok {
									feedTitle := "未知"
									if feedTitle, ok = metaData.(map[string]interface{})["feed"].(map[string]interface{})["title"].(map[string]interface{})["contents"].([]interface{})[0].(map[string]interface{})["text_content"].(map[string]interface{})["text"].(string); !ok {
										logger.Warnf("Failed to parse feed title")
									}
									metaMap = CardMessageMeta{
										Title: channel["channel_name"].(string),
										Desc:  feedTitle,
										Tag:   "频道",
									}
								} else {
									needDec = true
								}
							}
							if needDec {
								b, _ := json.Marshal(metaData)
								_ = json.Unmarshal(b, &metaMap)
							}
							title = metaMap.Title
							desc = metaMap.Desc
							tag = metaMap.Tag
							text += tag + "][" + title + "][" + desc + "]"
							if isGroupMsg {
								g.Elements = append(g.Elements, &message.LightAppElement{Content: text})
							} else {
								pMsg.Elements = append(pMsg.Elements, &message.LightAppElement{Content: text})
							}
						}
					default:
						logger.Errorf("Unknown card message type: %v", card)
					}
				}
			case "file":
				_, ok := contentMap["data"].(map[string]interface{})
				if ok {
					text := "[文件]" // + file["file"].(string)
					if isGroupMsg {
						g.Elements = append(g.Elements, &message.TextElement{Content: text})
					} else {
						pMsg.Elements = append(pMsg.Elements, &message.TextElement{Content: text})
					}
				}
			case "forward":
				forward, ok := contentMap["data"].(map[string]interface{})
				if ok {
					text := "[合并转发]" // + forward["id"].(string)
					msg := &message.ForwardElement{
						Content: text,
						ResId:   forward["id"].(string),
					}
					if isGroupMsg {
						g.Elements = append(g.Elements, msg)
					} else {
						pMsg.Elements = append(pMsg.Elements, msg)
					}
				}
			case "video":
				video, ok := contentMap["data"].(map[string]interface{})
				if ok {
					fileSize := 0
					var fileId []byte
					if video["file_size"] != nil {
						fileSize, _ = strconv.Atoi(video["file_size"].(string))
					}
					if video["file_id"] != nil {
						fileId = []byte(video["file_id"].(string))
					}
					msg := &message.ShortVideoElement{
						Name: video["file"].(string),
						Uuid: fileId,
						Size: int32(fileSize),
						Url:  video["url"].(string),
					}
					if isGroupMsg {
						g.Elements = append(g.Elements, msg)
					} else {
						pMsg.Elements = append(pMsg.Elements, msg)
					}
				}
			case "markdown":
				text := "[Markdown]"
				if isGroupMsg {
					g.Elements = append(g.Elements, &message.TextElement{Content: text})
				} else {
					pMsg.Elements = append(pMsg.Elements, &message.TextElement{Content: text})
				}
			default:
				logger.Warnf("Unknown content type: %s", contentType)
			}
		}
	}
	if isGroupMsg {
		c.waitDataAndDispatch(g)
	} else {
		selfIDStr := strconv.FormatInt(int64(wsmsg.SelfID), 10)
		if selfIDStr == strconv.FormatInt(int64(wsmsg.Sender.UserID), 10) {
			c.SelfPrivateMessageEvent.dispatch(c, pMsg)
		} else {
			c.PrivateMessageEvent.dispatch(c, pMsg)
		}
		c.OutputReceivingMessage(pMsg)
	}
}

func (c *QQClient) FindMemberByUin(groupCode int64, uin int64) (*GroupMemberInfo, error) {
	group := c.FindGroupByUin(groupCode)
	if group == nil {
		return nil, errors.New("group not found")
	}
	for _, member := range group.Members {
		if member.Uin == uin {
			return member, nil
		}
	}
	return nil, errors.New("member not found")
}

func (c *QQClient) GetGroupMemberInfo(groupCode int64, memberUin int64) (*GroupMemberInfo, error) {
	params := map[string]any{
		"group_id": groupCode,
		"user_id":  memberUin,
		"no_cache": false,
	}
	data, err := c.SendApi("get_group_member_info", params)
	if err != nil {
		return nil, err
	}
	bytes, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	var member MemberData
	var per MemberPermission
	var sex byte
	if err = json.Unmarshal(bytes, &member); err != nil {
		return nil, err
	}
	switch member.Role {
	case "owner":
		per = Owner
	case "admin":
		per = Administrator
	case "member":
		per = Member
	}
	switch member.Sex {
	case "male":
		sex = 2
	case "female":
		sex = 1
	case "unknown":
		sex = 0
	}
	ret := GroupMemberInfo{
		Group:           c.FindGroupByUin(groupCode),
		Uin:             member.UserID,
		Nickname:        member.Nickname,
		CardName:        member.Card,
		JoinTime:        member.JoinTime,
		LastSpeakTime:   member.LastSentTime,
		SpecialTitle:    member.Title,
		ShutUpTimestamp: member.ShutUpTimestamp,
		Permission:      per,
		Level:           uint16(member.QQLevel),
		Gender:          sex,
	}
	return &ret, nil
}

func (c *QQClient) SendApi(api string, params map[string]any, expTime ...float64) (any, error) {
	// 设置超时时间
	var timeout float64
	if len(expTime) > 0 {
		timeout = expTime[0]
	} else {
		timeout = 10
	}
	// 构建请求
	echo := generateEcho(api)
	req := map[string]any{
		"action": api,
		"params": params,
		"echo":   echo,
	}
	// 发送请求
	data, _ := json.Marshal(req)
	ch := make(chan *T)
	c.responseCh[echo] = ch
	c.sendToWebSocketClient(c.ws, data)
	// 等待返回
	select {
	case res := <-ch:
		if res.Status == "ok" {
			return res.Data, nil
		} else {
			return nil, errors.New(res.Message)
		}
	case <-time.After(time.Second * time.Duration(timeout)):
		return nil, errors.New(api + " timeout")
	}
}

// func (c *QQClient) SyncTickerControl(m int, wsmsg WebSocketMessage, flash bool) {
// allow:
// 	if c.limiterMessageSend.Grant() {
// 		logger.Debugf("流控：允许通过")
// 		if m == 1 {
// 			c.SyncGroupMembers(wsmsg.GroupID, flash)
// 		} else if m == 2 {
// 			c.ReloadFriendList()
// 		}
// 	} else {
// 		logger.Debugf("流控：拒绝通过")
// 		time.Sleep(time.Second * 1)
// 		goto allow
// 	}
// }

func (c *QQClient) OutputReceivingMessage(Msg interface{}) {
	mode := false
	var content []message.IMessageElement
	if msg, ok := Msg.(*message.GroupMessage); ok {
		content = msg.GetElements()
	} else if msg, ok := Msg.(*message.PrivateMessage); ok {
		content = msg.GetElements()
		mode = true
	}
	var tmpText string
	for _, elem := range content {
		if text, ok := elem.(*message.TextElement); ok {
			if len(text.Content) > 75 {
				tmpText += text.Content[:75] + "..."
			} else {
				tmpText += text.Content
			}
		} else if _, ok := elem.(*message.AtElement); ok {
			tmpText += "[艾特]"
		} else if _, ok := elem.(*message.FaceElement); ok {
			tmpText += "[表情]"
		} else if _, ok := elem.(*message.MarketFaceElement); ok {
			tmpText += "[表情]"
		} else if _, ok := elem.(*message.GroupImageElement); ok {
			tmpText += "[图片]"
		} else if _, ok := elem.(*message.FriendImageElement); ok {
			tmpText += "[图片]"
		} else if _, ok := elem.(*message.ReplyElement); ok {
			tmpText += "[引用]"
		} else if _, ok := elem.(*message.VoiceElement); ok {
			tmpText += "[语音]"
		} else if e, ok := elem.(*message.ForwardElement); ok {
			tmpText += e.Content
		} else if e, ok := elem.(*message.LightAppElement); ok {
			tmpText += e.Content
		} else if _, ok := elem.(*message.ShortVideoElement); ok {
			tmpText += "[视频]"
		}
	}
	if !mode {
		name := Msg.(*message.GroupMessage).Sender.CardName
		if name == "" {
			name = Msg.(*message.GroupMessage).Sender.Nickname
		}
		logger.Infof("收到群 %s(%d) 内 %s(%d) 的消息: %s (%d)", Msg.(*message.GroupMessage).GroupName, Msg.(*message.GroupMessage).GroupCode, name, Msg.(*message.GroupMessage).Sender.Uin, tmpText, Msg.(*message.GroupMessage).Id)
		//logger.Debugf("%+v", Msg.(*message.GroupMessage))
	} else {
		logger.Infof("收到 %s(%d) 的私聊消息: %s (%d)", Msg.(*message.PrivateMessage).Sender.Nickname, Msg.(*message.PrivateMessage).Sender.Uin, tmpText, Msg.(*message.PrivateMessage).Id)
		//logger.Debugf("%+v", Msg.(*message.PrivateMessage))
	}
}

func (c *QQClient) waitDataAndDispatch(g *message.GroupMessage) {
	if c.GroupList != nil {
		c.SetMsgGroupNames(g)
	}
	if c.FriendList != nil {
		c.SetFriend(g)
	}
	// 使用 dispatch 方法
	c.GroupMessageEvent.dispatch(c, g)
	c.OutputReceivingMessage(g)
}

func (c *QQClient) SetFriend(g *message.GroupMessage) {
	g.Sender.IsFriend = c.FindFriend(g.Sender.Uin) != nil
}

func (c *QQClient) SetMsgGroupNames(g *message.GroupMessage) {
	group := c.FindGroupByUin(g.GroupCode)
	if group != nil {
		g.GroupName = group.Name
	} else {
		logger.Warnf("Not found group name: %d", g.GroupCode)
	}
}

func (c *QQClient) SyncGroupMembers(groupID DynamicInt64) {
	//time.Sleep(time.Second * 3)
	// if flash {
	// 	//logger.Info("start reload groups list")
	// 	err := c.ReloadGroupList()
	// 	logger.Infof("load %d groups", len(c.GroupList))
	// 	if err != nil {
	// 		logger.WithError(err).Error("unable to load groups list")
	// 	}
	// 	err = c.ReloadGroupMembers()
	// 	if err != nil {
	// 		logger.WithError(err).Error("unable to load group members list")
	// 	} else {
	// 		logger.Info("load members done.")
	// 	}
	// } else {
	group := c.FindGroupByUin(groupID.ToInt64())
	// sort.Slice(c.GroupList, func(i2, j int) bool {
	// 	return c.GroupList[i2].Uin < c.GroupList[j].Uin
	// })
	// i := sort.Search(len(c.GroupList), func(i int) bool {
	// 	return c.GroupList[i].Uin >= int64(groupID)
	// })
	// if i >= len(c.GroupList) || c.GroupList[i].Uin != int64(groupID) {
	// 	return
	// }
	//c.GroupList[i].Members, err = c.getGroupMembers(c.GroupList[i], intern)
	if group != nil {
		var err error
		group.Members, err = c.GetGroupMembers(group)
		if err != nil {
			logger.Errorf("Failed to update group members: %v", err)
		}
	} else {
		logger.Warnf("Not found group: %d", groupID.ToInt64())
	}
	// }
}

func (c *QQClient) StartWebSocketServer() {
	wsMode := config.GlobalConfig.GetString("websocket.mode")
	if wsMode == "" {
		wsMode = "ws-server"
	}
	if wsMode == "ws-server" {
		http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
			ws, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				//log.Println(err)
				logger.Error(err)
				return
			}
			// 打印客户端的 headers
			for name, values := range r.Header {
				for _, value := range values {
					logger.WithField(name, value).Debug()
				}
			}
			c.wsInit(ws, wsMode)
		})
		// 启动监听
		wsAddr := config.GlobalConfig.GetString("websocket.ws-server")
		if wsAddr == "" {
			wsAddr = "0.0.0.0:15630"
		}
		logger.WithField("force", true).Printf("WebSocket server started on ws://%s/ws", wsAddr)
		logger.Fatal(http.ListenAndServe(wsAddr, nil))
	} else {
		c.disconnectChan = make(chan bool)
		go c.reverseConn(wsMode)
	}
}

func (c *QQClient) reverseConn(mode string) {
	var err error
	var ws *websocket.Conn
	// 使用反向链接
	wsAddr := config.GlobalConfig.GetString("websocket.ws-reverse")
	if wsAddr == "" {
		wsAddr = "ws://localhost:3001"
	}
	logger.WithField("force", true).Printf("WebSocket reverse started on %s", wsAddr)
	for {
		ws, _, err = websocket.DefaultDialer.Dial(wsAddr, nil)
		if err != nil {
			logger.Debug("dial:", err)
			time.Sleep(time.Second * 5)
			continue
		}
		c.wsInit(ws, mode)
		<-c.disconnectChan
		logger.Debug("Received disconnect signal, attempting to reconnect...")
	}
}

func (c *QQClient) wsInit(ws *websocket.Conn, mode string) {
	logger.Info("有新的ws连接了!!")
	//初始化变量
	c.ws = ws
	c.responseCh = make(map[string]chan *T)
	// 初始化流控
	c.limiterMessageSend = NewRateLimiter(time.Second, 5, func() Window {
		return NewLocalWindow()
	})
	// 刷新信息并启动流控和消息处理
	go c.RefreshList()
	go c.SendLimit()
	go c.processMessage()
	if mode == "ws-server" {
		c.handleConnection(ws)
	} else {
		go func() {
			c.handleConnection(ws)
			c.disconnectChan <- true // 连接断开时发送信号
		}()
	}
}

// RefreshList 刷新联系人
func (c *QQClient) RefreshList() {
	reloadDelay := config.GlobalConfig.GetBool("reloadDelay.enable")
	if reloadDelay {
		//logger.Info("enabled reload delay")
		var Delay time.Duration
		Delay = config.GlobalConfig.GetDuration("reloadDelay.time")
		logger.Infof("延迟加载时间: %ss", strconv.FormatFloat(Delay.Seconds(), 'f', 0, 64))
		time.Sleep(Delay)
	}
	//logger.Info("start reload friends list")
	err := c.ReloadFriendList()
	if err != nil {
		logger.WithError(err).Error("unable to load friends list")
	}
	logger.Infof("已加载 %d 个好友", len(c.FriendList))
	//logger.Info("start reload groups list")
	err = c.ReloadGroupList()
	if err != nil {
		logger.WithError(err).Error("unable to load groups list")
	}
	logger.Infof("已加载 %d 个群组", len(c.GroupList))
	//logger.Info("start reload group members list")
	err = c.ReloadGroupMembers()
	if err != nil {
		logger.WithError(err).Error("unable to load group members list")
	} else {
		logger.Info("加载 群员信息 完成")
	}
}

func (c *QQClient) ReloadGroupMembers() error {
	var err error
	for _, group := range c.GroupList {
		group.Members = nil
		group.Members, err = c.GetGroupMembers(group)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *QQClient) SendGroupMessage(groupCode int64, m *message.SendingMessage, newstr string) SendResp {
	resultChan := make(chan SendResp)
	sendMsg := SendMsg{
		GroupCode:  groupCode,
		Message:    m,
		NewStr:     newstr,
		ResultChan: resultChan,
	}
	sendMessageQueue <- sendMsg
	return <-resultChan
}

func (c *QQClient) sendToWebSocketClient(ws *websocket.Conn, message []byte) {
	if ws != nil {
		err := ws.WriteMessage(websocket.TextMessage, message)
		if err != nil {
			//log.Printf("Failed to send message to WebSocket client: %v", err)
			logger.Errorf("Failed to send message to WebSocket client: %v", err)
		}
	}
}

// NewClient create new qq client
func NewClient(uin int64, password string) *QQClient {
	return NewClientMd5(uin, md5.Sum([]byte(password)))
}

func NewClientEmpty() *QQClient {
	return NewClientMd5(0, [16]byte{})
}

func NewClientMd5(uin int64, passwordMd5 [16]byte) *QQClient {
	cli := &QQClient{
		Uin:         uin,
		PasswordMd5: passwordMd5,
		AllowSlider: true,
		TCP:         &network.TCPClient{},
		sig: &auth.SigInfo{
			OutPacketSessionID: []byte{0x02, 0xB0, 0x5B, 0x8B},
		},
		msgSvcCache:     utils.NewCache[unit](time.Second * 15),
		transCache:      utils.NewCache[unit](time.Second * 15),
		onlinePushCache: utils.NewCache[unit](time.Second * 15),
		alive:           true,
		highwaySession:  new(highway.Session),
	}

	go cli.StartWebSocketServer() // 异步启动WebSocket服务端

	cli.transport = &network.Transport{Sig: cli.sig}
	cli.oicq = oicq.NewCodec(cli.Uin)
	{ // init atomic values
		cli.SequenceId.Store(int32(rand.Intn(100000)))
		cli.requestPacketRequestID.Store(1921334513)
		cli.groupSeq.Store(int32(rand.Intn(20000)))
		cli.friendSeq.Store(22911)
		cli.highwayApplyUpSeq.Store(77918)
	}
	cli.highwaySession.Uin = strconv.FormatInt(cli.Uin, 10)
	cli.GuildService = &GuildService{c: cli}
	cli.TCP.PlannedDisconnect(cli.plannedDisconnect)
	cli.TCP.UnexpectedDisconnect(cli.unexpectedDisconnect)
	return cli
}

func (c *QQClient) version() *auth.AppVersion {
	return c.transport.Version
}

func (c *QQClient) Device() *DeviceInfo {
	return c.transport.Device
}

func (c *QQClient) UseDevice(info *auth.Device) {
	c.transport.Version = info.Protocol.Version()
	c.transport.Device = info
	c.highwaySession.AppID = int32(c.version().AppId)
	c.sig.Ksid = []byte(fmt.Sprintf("|%s|A8.2.7.27f6ea96", info.IMEI))
}

func (c *QQClient) Release() {
	if c.Online.Load() {
		c.Disconnect()
	}
	c.alive = false
}

// Login send login request
func (c *QQClient) Login() (*LoginResponse, error) {
	if c.Online.Load() {
		return nil, ErrAlreadyOnline
	}
	err := c.connect()
	if err != nil {
		return nil, err
	}
	rsp, err := c.sendAndWait(c.buildLoginPacket())
	if err != nil {
		c.Disconnect()
		return nil, err
	}
	l := rsp.(LoginResponse)
	if l.Success {
		err = c.init(false)
	}
	return &l, err
}

func (c *QQClient) TokenLogin(token []byte) error {
	if c.Online.Load() {
		return ErrAlreadyOnline
	}
	err := c.connect()
	if err != nil {
		return err
	}
	{
		r := binary.NewReader(token)
		c.Uin = r.ReadInt64()
		c.sig.D2 = r.ReadBytesShort()
		c.sig.D2Key = r.ReadBytesShort()
		c.sig.TGT = r.ReadBytesShort()
		c.sig.SrmToken = r.ReadBytesShort()
		c.sig.T133 = r.ReadBytesShort()
		c.sig.EncryptedA1 = r.ReadBytesShort()
		c.oicq.WtSessionTicketKey = r.ReadBytesShort()
		c.sig.OutPacketSessionID = r.ReadBytesShort()
		// SystemDeviceInfo.TgtgtKey = r.ReadBytesShort()
		c.Device().TgtgtKey = r.ReadBytesShort()
	}
	_, err = c.sendAndWait(c.buildRequestChangeSigPacket(true))
	if err != nil {
		return err
	}
	return c.init(true)
}

func (c *QQClient) FetchQRCode() (*QRCodeLoginResponse, error) {
	return c.FetchQRCodeCustomSize(3, 4, 2)
}

func (c *QQClient) FetchQRCodeCustomSize(size, margin, ecLevel uint32) (*QRCodeLoginResponse, error) {
	if c.Online.Load() {
		return nil, ErrAlreadyOnline
	}
	err := c.connect()
	if err != nil {
		return nil, err
	}
	i, err := c.sendAndWait(c.buildQRCodeFetchRequestPacket(size, margin, ecLevel))
	if err != nil {
		return nil, errors.Wrap(err, "fetch qrcode error")
	}
	return i.(*QRCodeLoginResponse), nil
}

func (c *QQClient) QueryQRCodeStatus(sig []byte) (*QRCodeLoginResponse, error) {
	i, err := c.sendAndWait(c.buildQRCodeResultQueryRequestPacket(sig))
	if err != nil {
		return nil, errors.Wrap(err, "query result error")
	}
	return i.(*QRCodeLoginResponse), nil
}

func (c *QQClient) QRCodeLogin(info *QRCodeLoginInfo) (*LoginResponse, error) {
	i, err := c.sendAndWait(c.buildQRCodeLoginPacket(info.tmpPwd, info.tmpNoPicSig, info.tgtQR))
	if err != nil {
		return nil, errors.Wrap(err, "qrcode login error")
	}
	rsp := i.(LoginResponse)
	if rsp.Success {
		err = c.init(false)
	}
	return &rsp, err
}

// SubmitCaptcha send captcha to server
func (c *QQClient) SubmitCaptcha(result string, sign []byte) (*LoginResponse, error) {
	seq, packet := c.buildCaptchaPacket(result, sign)
	rsp, err := c.sendAndWait(seq, packet)
	if err != nil {
		c.Disconnect()
		return nil, err
	}
	l := rsp.(LoginResponse)
	if l.Success {
		err = c.init(false)
	}
	return &l, err
}

func (c *QQClient) SubmitTicket(ticket string) (*LoginResponse, error) {
	seq, packet := c.buildTicketSubmitPacket(ticket)
	rsp, err := c.sendAndWait(seq, packet)
	if err != nil {
		c.Disconnect()
		return nil, err
	}
	l := rsp.(LoginResponse)
	if l.Success {
		err = c.init(false)
	}
	return &l, err
}

func (c *QQClient) SubmitSMS(code string) (*LoginResponse, error) {
	rsp, err := c.sendAndWait(c.buildSMSCodeSubmitPacket(code))
	if err != nil {
		c.Disconnect()
		return nil, err
	}
	l := rsp.(LoginResponse)
	if l.Success {
		err = c.init(false)
	}
	return &l, err
}

func (c *QQClient) RequestSMS() bool {
	rsp, err := c.sendAndWait(c.buildSMSRequestPacket())
	if err != nil {
		c.error("request sms error: %v", err)
		return false
	}
	return rsp.(LoginResponse).Error == SMSNeededError
}

func (c *QQClient) init(tokenLogin bool) error {
	if len(c.sig.G) == 0 {
		c.warning("device lock is disabled. HTTP API may fail.")
	}
	c.highwaySession.Uin = strconv.FormatInt(c.Uin, 10)
	if err := c.registerClient(); err != nil {
		return errors.Wrap(err, "register error")
	}
	if tokenLogin {
		notify := make(chan struct{}, 2)
		d := c.waitPacket("StatSvc.ReqMSFOffline", func(i any, err error) {
			notify <- struct{}{}
		})
		d2 := c.waitPacket("MessageSvc.PushForceOffline", func(i any, err error) {
			notify <- struct{}{}
		})
		select {
		case <-notify:
			d()
			d2()
			return errors.New("token failed")
		case <-time.After(time.Second):
			d()
			d2()
		}
	}
	c.groupSysMsgCache, _ = c.GetGroupSystemMessages()
	if !c.heartbeatEnabled {
		go c.doHeartbeat()
	}
	_ = c.RefreshStatus()
	if c.version().Protocol == auth.QiDian {
		_, _ = c.sendAndWait(c.buildLoginExtraPacket())     // 小登录
		_, _ = c.sendAndWait(c.buildConnKeyRequestPacket()) // big data key 如果等待 config push 的话时间来不及
	}
	seq, pkt := c.buildGetMessageRequestPacket(msg.SyncFlag_START, time.Now().Unix())
	_, _ = c.sendAndWait(seq, pkt, network.RequestParams{"used_reg_proxy": true, "init": true})
	c.syncChannelFirstView()
	return nil
}

func (c *QQClient) GenToken() []byte {
	return binary.NewWriterF(func(w *binary.Writer) {
		w.WriteUInt64(uint64(c.Uin))
		w.WriteBytesShort(c.sig.D2)
		w.WriteBytesShort(c.sig.D2Key)
		w.WriteBytesShort(c.sig.TGT)
		w.WriteBytesShort(c.sig.SrmToken)
		w.WriteBytesShort(c.sig.T133)
		w.WriteBytesShort(c.sig.EncryptedA1)
		w.WriteBytesShort(c.oicq.WtSessionTicketKey)
		w.WriteBytesShort(c.sig.OutPacketSessionID)
		w.WriteBytesShort(c.Device().TgtgtKey)
	})
}

func (c *QQClient) SetOnlineStatus(s UserOnlineStatus) {
	if s < 1000 {
		_, _ = c.sendAndWait(c.buildStatusSetPacket(int32(s), 0))
		return
	}
	_, _ = c.sendAndWait(c.buildStatusSetPacket(11, int32(s)))
}

func (c *QQClient) GetWordSegmentation(text string) ([]string, error) {
	rsp, err := c.sendAndWait(c.buildWordSegmentationPacket([]byte(text)))
	if err != nil {
		return nil, err
	}
	if data, ok := rsp.([][]byte); ok {
		var ret []string
		for _, val := range data {
			ret = append(ret, string(val))
		}
		return ret, nil
	}
	return nil, errors.New("decode error")
}

func (c *QQClient) GetSummaryInfo(target int64) (*SummaryCardInfo, error) {
	rsp, err := c.sendAndWait(c.buildSummaryCardRequestPacket(target))
	if err != nil {
		return nil, err
	}
	return rsp.(*SummaryCardInfo), nil
}

// ReloadFriendList refresh QQClient.FriendList field via GetFriendList()
func (c *QQClient) ReloadFriendList() error {
	rsp, err := c.GetFriendList()
	if err != nil {
		return err
	}
	c.FriendList = rsp
	return nil
}

func (c *QQClient) GetFriendList() ([]*FriendInfo, error) {
	data, err := c.SendApi("get_friend_list", nil)
	if err != nil {
		return nil, err
	}
	t, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	var resp []FriendData
	if err := json.Unmarshal(t, &resp); err != nil {
		return nil, err
	}
	friends := make([]*FriendInfo, len(resp))
	c.debug("GetFriendList: %v", resp)
	for i, friend := range resp {
		friends[i] = &FriendInfo{
			Uin:      friend.UserID,
			Nickname: friend.NickName,
			Remark:   friend.Remark,
			FaceId:   0,
		}
	}
	return friends, nil
	// echo := generateEcho("get_friend_list")
	// 构建请求
	// req := map[string]interface{}{
	// 	"action": "get_friend_list",
	// 	"params": map[string]int64{},
	// 	"echo":   echo,
	// }

	// 创建响应通道并添加到映射中
	// respChan := make(chan *ResponseFriendList)
	// c.responseFriends[echo] = respChan

	// 发送请求
	// data, _ := json.Marshal(req)
	// c.sendToWebSocketClient(c.ws, data)

	// 等待响应或超时
	// select {
	// case resp := <-respChan:
	// 	delete(c.responseFriends, echo)
	// 	friends := make([]*FriendInfo, len(resp.Data))
	// 	c.debug("GetFriendList: %v", resp.Data)
	// 	for i, friend := range resp.Data {
	// 		friends[i] = &FriendInfo{
	// 			Uin:      friend.UserID,
	// 			Nickname: friend.NickName,
	// 			Remark:   friend.Remark,
	// 			FaceId:   0,
	// 		}
	// 	}
	// 	return &FriendListResponse{TotalCount: int32(len(friends)), List: friends}, nil

	// case <-time.After(10 * time.Second):
	// 	delete(c.responseFriends, echo)
	// 	return nil, errors.New("GetFriendList: timeout waiting for response.")
	// }
}

// GetFriendList
// 当使用普通QQ时: 请求好友列表
// 当使用企点QQ时: 请求外部联系人列表
// func (c *QQClient) GetFriendList() (*FriendListResponse, error) {
// 	if c.version().Protocol == auth.QiDian {
// 		rsp, err := c.getQiDianAddressDetailList()
// 		if err != nil {
// 			return nil, err
// 		}
// 		return &FriendListResponse{TotalCount: int32(len(rsp)), List: rsp}, nil
// 	}
// 	curFriendCount := 0
// 	r := &FriendListResponse{}
// 	for {
// 		rsp, err := c.sendAndWait(c.buildFriendGroupListRequestPacket(int16(curFriendCount), 150, 0, 0))
// 		if err != nil {
// 			return nil, err
// 		}
// 		list := rsp.(*FriendListResponse)
// 		r.TotalCount = list.TotalCount
// 		r.List = append(r.List, list.List...)
// 		curFriendCount += len(list.List)
// 		if int32(len(r.List)) >= r.TotalCount {
// 			break
// 		}
// 	}
// 	return r, nil
// }

func (c *QQClient) GetGroupInfo(groupCode int64) (*GroupInfo, error) {
	data, err := c.SendApi("get_group_info", map[string]any{"group_id": groupCode})
	if err != nil {
		return nil, err
	}
	var resp GroupData
	t, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(t, &resp); err != nil {
		return nil, err
	}
	return &GroupInfo{
		Uin:             resp.GroupID,
		Code:            resp.GroupID,
		Name:            resp.GroupName,
		GroupCreateTime: uint32(resp.GroupCreateTime),
		GroupLevel:      uint32(resp.GroupLevel),
		MemberCount:     uint16(resp.MemberCount),
		MaxMemberCount:  uint16(resp.MaxMemberCount),
	}, nil
	// echo := generateEcho("get_group_info")
	// // 构建请求
	// req := map[string]interface{}{
	// 	"action": "get_group_info",
	// 	"params": map[string]int64{
	// 		"group_id": groupCode,
	// 	},
	// 	"echo": echo,
	// }
	// // 创建响应通道并添加到映射中
	// respChan := make(chan *ResponseGroupData)
	// c.responseGroupChans[echo] = respChan

	// // 发送请求
	// data, _ := json.Marshal(req)
	// c.sendToWebSocketClient(c.ws, data)

	// // 等待响应或超时
	// select {
	// case resp := <-respChan:
	// 	if resp.Retcode != 0 {
	// 		return nil, errors.New(resp.Message)
	// 	}
	// 	delete(c.responseGroupChans, echo)
	// 	c.debug("GetGroupInfo: %v", resp.Data)
	// 	group := &GroupInfo{
	// 		Uin:             resp.Data.GroupID,
	// 		Code:            resp.Data.GroupID,
	// 		Name:            resp.Data.GroupName,
	// 		GroupCreateTime: uint32(resp.Data.GroupCreateTime),
	// 		GroupLevel:      uint32(resp.Data.GroupLevel),
	// 		MemberCount:     uint16(resp.Data.MemberCount),
	// 		MaxMemberCount:  uint16(resp.Data.MaxMemberCount),
	// 	}
	// 	return group, nil
	// case <-time.After(10 * time.Second):
	// 	delete(c.responseGroupChans, echo)
	// 	return nil, errors.New("GetGroupInfo: timeout waiting for response.")
	// }
}

func (c *QQClient) SendGroupPoke(groupCode, target int64) {
	_, _ = c.sendAndWait(c.buildGroupPokePacket(groupCode, target))
}

func (c *QQClient) SendFriendPoke(target int64) {
	_, _ = c.sendAndWait(c.buildFriendPokePacket(target))
}

func (c *QQClient) ReloadGroupList() error {
	c.groupListLock.Lock()
	defer c.groupListLock.Unlock()
	list, err := c.GetGroupList()
	if err != nil {
		return err
	}
	c.GroupList = list
	return nil
}

func generateEcho(action string) string {
	timestamp := time.Now().UnixNano()
	return fmt.Sprintf("%s:%d", action, timestamp)
}

func (c *QQClient) GetGroupList() ([]*GroupInfo, error) {
	data, err := c.SendApi("get_group_list", nil)
	if err != nil {
		return nil, err
	}
	var resp []GroupData
	t, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(t, &resp); err != nil {
		return nil, err
	}
	groups := make([]*GroupInfo, len(resp))
	for i, group := range resp {
		groups[i] = &GroupInfo{
			Uin:             group.GroupID,
			Code:            group.GroupID,
			Name:            group.GroupName,
			GroupCreateTime: uint32(group.GroupCreateTime),
			GroupLevel:      uint32(group.GroupLevel),
			MemberCount:     uint16(group.MemberCount),
			MaxMemberCount:  uint16(group.MaxMemberCount),
		}
	}
	return groups, nil
	// echo := generateEcho("get_group_list")
	// // 构建请求
	// req := map[string]interface{}{
	// 	"action": "get_group_list",
	// 	"params": map[string]string{},
	// 	"echo":   echo,
	// }
	// // 创建响应通道并添加到映射中
	// respChan := make(chan *ResponseGroupsData)
	// // 初始化 c.responseChans 映射
	// c.responseChans[echo] = respChan

	// // 发送请求
	// data, _ := json.Marshal(req)
	// c.sendToWebSocketClient(c.ws, data)

	// // 等待响应或超时
	// select {
	// case resp := <-respChan:
	// 	// 根据resp的结构处理数据
	// 	//if resp.Retcode != 0 || resp.Status != "ok" {
	// 	//	return nil, fmt.Errorf("error response from server: %s", resp.Message)
	// 	//}

	// 	// 将ResponseGroupsData转换为GroupInfo列表
	// 	delete(c.responseChans, echo)
	// 	groups := make([]*GroupInfo, len(resp.Data))
	// 	c.debug("GetGroupList: %v", resp.Data)
	// 	for i, groupData := range resp.Data {
	// 		groups[i] = &GroupInfo{
	// 			Uin:             groupData.GroupID,
	// 			Code:            groupData.GroupID,
	// 			Name:            groupData.GroupName,
	// 			GroupCreateTime: uint32(groupData.GroupCreateTime),
	// 			GroupLevel:      uint32(groupData.GroupLevel),
	// 			MemberCount:     uint16(groupData.MemberCount),
	// 			MaxMemberCount:  uint16(groupData.MaxMemberCount),
	// 			// TODO: add more fields if necessary, like Members, etc.
	// 		}
	// 	}

	// 	return groups, nil

	// case <-time.After(10 * time.Second):
	// 	delete(c.responseChans, echo)
	// 	return nil, errors.New("GetGroupList: timeout waiting for response.")
	// }

	// TODO:
	// 返回GroupInfo结构体列表

	// rsp, err := c.sendAndWait(c.buildGroupListRequestPacket(EmptyBytes))
	// if err != nil {
	// 	return nil, err
	// }
	// interner := intern.NewStringInterner()
	// r := rsp.([]*GroupInfo)
	// wg := sync.WaitGroup{}
	// batch := 50
	// for i := 0; i < len(r); i += batch {
	// 	k := i + batch
	// 	if k > len(r) {
	// 		k = len(r)
	// 	}
	// 	wg.Add(k - i)
	// 	for j := i; j < k; j++ {
	// 		go func(g *GroupInfo, wg *sync.WaitGroup) {
	// 			defer wg.Done()
	// 			m, err := c.getGroupMembers(g, interner)
	// 			if err != nil {
	// 				return
	// 			}
	// 			g.Members = m
	// 			g.Name = interner.Intern(g.Name)
	// 		}(r[j], &wg)
	// 	}
	// 	wg.Wait()
	// }
	// return r, nil

	//虚拟数据
	// logger.WithField("force", true).Print("欺骗ddbot,给它假的群数据")
	// fmt.Printf("欺骗ddbot,给它假的群数据")
	// groups := []*GroupInfo{
	// 	{
	// 		Uin:             670078416,
	// 		Code:            670078416,
	// 		Name:            "TestGroup",
	// 		OwnerUin:        2022717137,
	// 		GroupCreateTime: 1634000000,
	// 		GroupLevel:      1,
	// 		MemberCount:     3,
	// 		MaxMemberCount:  100,
	// 		Members: []*GroupMemberInfo{
	// 			{
	// 				Uin:             12345678,
	// 				Nickname:        "Member1",
	// 				CardName:        "Card1",
	// 				JoinTime:        1633900000,
	// 				LastSpeakTime:   1633999999,
	// 				SpecialTitle:    "Title1",
	// 				ShutUpTimestamp: 0,
	// 				Permission:      0,
	// 				Level:           1,
	// 				Gender:          1,
	// 			},
	// 			{
	// 				Uin:             2022717137,
	// 				Nickname:        "Owner",
	// 				CardName:        "OwnerCard",
	// 				JoinTime:        1633700000,
	// 				LastSpeakTime:   1633799999,
	// 				SpecialTitle:    "OwnerTitle",
	// 				ShutUpTimestamp: 0,
	// 				Permission:      1,
	// 				Level:           3,
	// 				Gender:          1,
	// 			},
	// 		},
	// 		LastMsgSeq: 10000,
	// 		client:     c,
	// 	},
	// }
	// return groups, nil
}

func (c *QQClient) GetGroupMembers(group *GroupInfo) ([]*GroupMemberInfo, error) {
	interner := intern.NewStringInterner()
	return c.getGroupMembers(group, interner)
}

func (c *QQClient) getGroupMembers(group *GroupInfo, interner *intern.StringInterner) ([]*GroupMemberInfo, error) {
	data, err := c.SendApi("get_group_member_list", map[string]any{
		"group_id": group.Uin,
	})
	if err != nil {
		return nil, err
	}
	var resp []MemberData
	t, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(t, &resp); err != nil {
		return nil, err
	}
	members := make([]*GroupMemberInfo, len(resp))
	for i, member := range resp {
		var permission MemberPermission
		if member.Role == "owner" {
			permission = Owner
		} else if member.Role == "admin" {
			permission = Administrator
		} else if member.Role == "member" {
			permission = Member
		}
		members[i] = &GroupMemberInfo{
			Group:           group,
			Uin:             member.UserID,
			Nickname:        member.Nickname,
			CardName:        member.Card,
			JoinTime:        member.JoinTime,
			LastSpeakTime:   member.LastSentTime,
			SpecialTitle:    member.Title,
			ShutUpTimestamp: member.ShutUpTimestamp,
			Permission:      permission,
		}
	}
	return members, nil
	// list := make([]*GroupMemberInfo, 0)
	// echo := generateEcho("get_group_member_list")
	// // 构建请求
	// req := map[string]interface{}{
	// 	"action": "get_group_member_list",
	// 	"params": map[string]int64{
	// 		"group_id": group.Uin,
	// 	},
	// 	"echo": echo,
	// }
	// // 创建响应通道并添加到映射中
	// respChan := make(chan *ResponseGroupMemberData)
	// // 初始化 c.responseChans 映射
	// c.responseMembers[echo] = respChan

	// // 发送请求
	// data, _ := json.Marshal(req)
	// c.sendToWebSocketClient(c.ws, data)

	// // 等待响应或超时
	// select {
	// case resp := <-respChan:
	// 	delete(c.responseMembers, echo)
	// 	members := make([]*GroupMemberInfo, len(resp.Data))
	// 	c.debug("GetGroupMembers: %v", resp.Data)
	// 	for i, memberData := range resp.Data {
	// 		var permission MemberPermission
	// 		if memberData.Role == "owner" {
	// 			permission = 1
	// 		} else if memberData.Role == "admin" {
	// 			permission = 2
	// 		} else if memberData.Role == "member" {
	// 			permission = 3
	// 		}
	// 		members[i] = &GroupMemberInfo{
	// 			Group:           group,
	// 			Uin:             memberData.UserID,
	// 			Nickname:        memberData.Nickname,
	// 			CardName:        memberData.Card,
	// 			JoinTime:        memberData.JoinTime,
	// 			LastSpeakTime:   memberData.LastSentTime,
	// 			SpecialTitle:    memberData.Title,
	// 			ShutUpTimestamp: memberData.ShutUpTimestamp,
	// 			Permission:      permission,
	// 		}
	// 	}

	// 	return members, nil

	// case <-time.After(10 * time.Second):
	// 	delete(c.responseMembers, echo)
	// 	return nil, errors.New("GetGroupMembers: timeout waiting for response.")
	// }

	// for {
	// 	data, err := c.sendAndWait(c.buildGroupMemberListRequestPacket(group.Uin, group.Code, nextUin))
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	if data == nil {
	// 		return nil, errors.New("group members list is unavailable: rsp is nil")
	// 	}
	// 	rsp := data.(*groupMemberListResponse)
	// 	nextUin = rsp.NextUin
	// 	for _, m := range rsp.list {
	// 		m.Group = group
	// 		if m.Uin == group.OwnerUin {
	// 			m.Permission = Owner
	// 		}
	// 		m.CardName = interner.Intern(m.CardName)
	// 		m.Nickname = interner.Intern(m.Nickname)
	// 		m.SpecialTitle = interner.Intern(m.SpecialTitle)
	// 	}
	// 	list = append(list, rsp.list...)
	// 	if nextUin == 0 {
	// 		sort.Slice(list, func(i, j int) bool {
	// 			return list[i].Uin < list[j].Uin
	// 		})
	// 		return list, nil
	// 	}
	// }
}

func (c *QQClient) GetMemberInfo(groupCode, memberUin int64) (*GroupMemberInfo, error) {
	info, err := c.sendAndWait(c.buildGroupMemberInfoRequestPacket(groupCode, memberUin))
	if err != nil {
		return nil, err
	}
	return info.(*GroupMemberInfo), nil
}

func (c *QQClient) FindFriend(uin int64) *FriendInfo {
	if uin == c.Uin {
		return &FriendInfo{
			Uin:      uin,
			Nickname: c.Nickname,
		}
	}
	for _, t := range c.FriendList {
		f := t
		if f.Uin == uin {
			return f
		}
	}
	return nil
}

func (c *QQClient) DeleteFriend(uin int64) error {
	if c.FindFriend(uin) == nil {
		return errors.New("friend not found")
	}
	_, err := c.sendAndWait(c.buildFriendDeletePacket(uin))
	return errors.Wrap(err, "delete friend error")
}

func (c *QQClient) FindGroupByUin(uin int64) *GroupInfo {
	for _, g := range c.GroupList {
		f := g
		if f.Uin == uin {
			return f
		}
	}
	return nil
}

func (c *QQClient) FindGroup(code int64) *GroupInfo {
	for _, g := range c.GroupList {
		if g.Code == code {
			return g
		}
	}
	return nil
}

func (c *QQClient) SolveGroupJoinRequest(i any, accept, block bool, reason string) {
	if accept {
		block = false
		reason = ""
	}
	subType := ""
	var Req interface{}
	switch req := i.(type) {
	case *UserJoinGroupRequest:
		// _, pkt := c.buildSystemMsgGroupActionPacket(req.RequestId, req.RequesterUin, req.GroupCode, func() int32 {
		// 	if req.Suspicious {
		// 		return 2
		// 	} else {
		// 		return 1
		// 	}
		// }(), false, accept, block, reason)
		// _ = c.sendPacket(pkt)
		subType = "add"
		Req = req
	case *GroupInvitedRequest:
		// _, pkt := c.buildSystemMsgGroupActionPacket(req.RequestId, req.InvitorUin, req.GroupCode, 1, true, accept, block, reason)
		// _ = c.sendPacket(pkt)
		subType = "invite"
		Req = req
	}
	// echo := generateEcho("set_group_add_request")
	// // 构建请求
	// newReq := map[string]interface{}{
	// 	"action": "set_group_add_request",
	// 	"params": map[string]interface{}{
	// 		"sub_type": subType,
	// 		"approve":  accept,
	// 		"reason":   reason,
	// 	},
	// 	"echo": echo,
	// }
	// if _, ok := Req.(*UserJoinGroupRequest); ok {
	// 	newReq["params"].(map[string]interface{})["flag"] = Req.(*UserJoinGroupRequest).Flag
	// } else if _, ok := Req.(*GroupInvitedRequest); ok {
	// 	newReq["params"].(map[string]interface{})["flag"] = Req.(*GroupInvitedRequest).Flag
	// }
	// logger.Debugf("加群回复构建完成：%s", newReq)
	// // 发送请求
	// data, _ := json.Marshal(newReq)
	// c.sendToWebSocketClient(c.ws, data)
	params := map[string]any{
		"sub_type": subType,
		"approve":  accept,
		"reason":   reason,
	}
	if req, ok := Req.(*UserJoinGroupRequest); ok {
		params["flag"] = req.Flag
	} else if req, ok := Req.(*GroupInvitedRequest); ok {
		params["flag"] = req.Flag
	}
	_, _ = c.SendApi("set_group_add_request", params)
}

func (c *QQClient) SolveFriendRequest(req *NewFriendRequest, accept bool) {
	_, _ = c.SendApi("set_friend_add_request", map[string]any{
		"flag":    req.Flag,
		"approve": accept,
	})
	// echo := generateEcho("set_friend_add_request")
	// // 构建请求
	// newReq := map[string]interface{}{
	// 	"action": "set_friend_add_request",
	// 	"params": map[string]interface{}{
	// 		"flag":    req.Flag,
	// 		"approve": accept,
	// 	},
	// 	"echo": echo,
	// }
	// // 发送请求
	// data, _ := json.Marshal(newReq)
	// c.sendToWebSocketClient(c.ws, data)
}

// func (c *QQClient) SolveFriendRequest(req *NewFriendRequest, accept bool) {
// 	_, pkt := c.buildSystemMsgFriendActionPacket(req.RequestId, req.RequesterUin, accept)
// 	_ = c.sendPacket(pkt)
// }

func (c *QQClient) getSKey() string {
	if c.sig.SKeyExpiredTime < time.Now().Unix() && len(c.sig.G) > 0 {
		c.debug("skey expired. refresh...")
		_, _ = c.sendAndWait(c.buildRequestTgtgtNopicsigPacket())
	}
	return string(c.sig.SKey)
}

func (c *QQClient) getCookies() string {
	return fmt.Sprintf("uin=o%d; skey=%s;", c.Uin, c.getSKey())
}

func (c *QQClient) getCookiesWithDomain(domain string) string {
	cookie := c.getCookies()

	if psKey, ok := c.sig.PsKeyMap[domain]; ok {
		return fmt.Sprintf("%s p_uin=o%d; p_skey=%s;", cookie, c.Uin, psKey)
	} else {
		return cookie
	}
}

func (c *QQClient) getCSRFToken() int {
	accu := 5381
	for _, b := range []byte(c.getSKey()) {
		accu = accu + (accu << 5) + int(b)
	}
	return 2147483647 & accu
}

func (c *QQClient) editMemberCard(groupCode, memberUin int64, card string) {
	_, _ = c.sendAndWait(c.buildEditGroupTagPacket(groupCode, memberUin, card))
}

func (c *QQClient) editMemberSpecialTitle(groupCode, memberUin int64, title string) {
	_, _ = c.sendAndWait(c.buildEditSpecialTitlePacket(groupCode, memberUin, title))
}

func (c *QQClient) setGroupAdmin(groupCode, memberUin int64, flag bool) {
	_, _ = c.sendAndWait(c.buildGroupAdminSetPacket(groupCode, memberUin, flag))
}

func (c *QQClient) updateGroupName(groupCode int64, newName string) {
	_, _ = c.sendAndWait(c.buildGroupNameUpdatePacket(groupCode, newName))
}

func (c *QQClient) groupMuteAll(groupCode int64, mute bool) {
	_, _ = c.sendAndWait(c.buildGroupMuteAllPacket(groupCode, mute))
}

func (c *QQClient) groupMute(groupCode, memberUin int64, time uint32) {
	_, _ = c.sendAndWait(c.buildGroupMutePacket(groupCode, memberUin, time))
}

func (c *QQClient) quitGroup(group *GroupInfo) {
	_, err := c.SendApi("set_group_leave", map[string]any{
		"group_id":   group.Code,
		"is_dismiss": false,
	})
	if err != nil {
		logger.Error(err)
		return
	}
	c.GroupLeaveEvent.dispatch(c, &GroupLeaveEvent{
		Group:    group,
		Operator: nil,
	})
	// _, pkt := c.buildSetGroupLeavePacket(group.Code, false)
	// _ = c.sendPacket(pkt)
	// echo := generateEcho("set_group_leave")
	// // 构建请求
	// req := map[string]interface{}{
	// 	"action": "set_group_leave",
	// 	"params": map[string]interface{}{
	// 		"group_id":   group.Code,
	// 		"is_dismiss": false,
	// 	},
	// 	"echo": echo,
	// }
	// // 创建响应通道并添加到映射中
	// respChan := make(chan *ResponseSetGroupLeave)
	// // 初始化 c.responseChans 映射
	// c.responseSetLeave[echo] = respChan
	// // 发送请求
	// data, _ := json.Marshal(req)
	// c.sendToWebSocketClient(c.ws, data)
	// // 等待响应或超时
	// select {
	// case resp := <-respChan:
	// 	delete(c.responseSetLeave, echo)
	// 	if resp.Retcode == 0 {
	// 		sort.Slice(c.GroupList, func(i, j int) bool {
	// 			return c.GroupList[i].Code < c.GroupList[j].Code
	// 		})
	// 		i := sort.Search(len(c.GroupList), func(i int) bool {
	// 			return c.GroupList[i].Code >= group.Code
	// 		})
	// 		if i < len(c.GroupList) && c.GroupList[i].Code == group.Code {
	// 			c.GroupList = append(c.GroupList[:i], c.GroupList[i+1:]...)
	// 		}
	// 		logger.Infof("退出群组成功")
	// 	} else {
	// 		logger.Errorf("退出群组失败: %v", resp.Message)
	// 	}
	// case <-time.After(10 * time.Second):
	// 	delete(c.responseSetLeave, echo)
	// 	logger.Errorf("退出群组超时")
	// }
}

func (c *QQClient) KickGroupMembers(groupCode int64, msg string, block bool, memberUins ...int64) {
	_, _ = c.sendAndWait(c.buildGroupKickPacket(groupCode, msg, block, memberUins...))
}

func (g *GroupInfo) removeMember(uin int64) {
	g.Update(func(info *GroupInfo) {
		i := sort.Search(len(info.Members), func(i int) bool {
			return info.Members[i].Uin >= uin
		})
		if i >= len(info.Members) || info.Members[i].Uin != uin { // not found
			return
		}
		info.Members = append(info.Members[:i], info.Members[i+1:]...)
	})
}

func (c *QQClient) setGroupAnonymous(groupCode int64, enable bool) {
	_, _ = c.sendAndWait(c.buildSetGroupAnonymous(groupCode, enable))
}

// UpdateProfile 修改个人资料
func (c *QQClient) UpdateProfile(profile ProfileDetailUpdate) {
	_, _ = c.sendAndWait(c.buildUpdateProfileDetailPacket(profile))
}

func (c *QQClient) SetCustomServer(servers []netip.AddrPort) {
	c.servers = append(servers, c.servers...)
}

func (c *QQClient) registerClient() error {
	_, err := c.sendAndWait(c.buildClientRegisterPacket())
	if err == nil {
		c.Online.Store(true)
	}
	return err
}

func (c *QQClient) nextSeq() uint16 {
	seq := c.SequenceId.Add(1)
	if seq > 1000000 {
		seq = int32(rand.Intn(100000)) + 60000
		c.SequenceId.Store(seq)
	}
	return uint16(seq)
}

func (c *QQClient) nextPacketSeq() int32 {
	return c.requestPacketRequestID.Add(2)
}

func (c *QQClient) nextGroupSeq() int32 {
	return c.groupSeq.Add(2)
}

func (c *QQClient) nextFriendSeq() int32 {
	return c.friendSeq.Add(1)
}

func (c *QQClient) nextQWebSeq() int64 {
	return c.qwebSeq.Add(1)
}

func (c *QQClient) nextHighwayApplySeq() int32 {
	return c.highwayApplyUpSeq.Add(2)
}

func (c *QQClient) doHeartbeat() {
	c.heartbeatEnabled = true
	times := 0
	for c.Online.Load() {
		time.Sleep(time.Second * 30)
		seq := c.nextSeq()
		req := network.Request{
			Type:        network.RequestTypeLogin,
			EncryptType: network.EncryptTypeNoEncrypt,
			SequenceID:  int32(seq),
			Uin:         c.Uin,
			CommandName: "Heartbeat.Alive",
			Body:        EmptyBytes,
		}
		packet := c.transport.PackPacket(&req)
		_, err := c.sendAndWait(seq, packet)
		if errors.Is(err, network.ErrConnectionClosed) {
			continue
		}
		times++
		if times >= 7 {
			_ = c.registerClient()
			times = 0
		}
	}
	c.heartbeatEnabled = false
}

func (c *QQClient) GetStrangerInfo(uid int64) (StrangerInfo, error) {
	data, err := c.SendApi("get_stranger_info", map[string]any{
		"user_id":  uid,
		"no_cache": false,
	})
	if err != nil {
		return StrangerInfo{}, err
	}
	t, err := json.Marshal(data)
	if err != nil {
		return StrangerInfo{}, err
	}
	var resp StrangerInfo
	if err = json.Unmarshal(t, &resp); err != nil {
		return StrangerInfo{}, err
	}
	return resp, nil
	// echo := generateEcho("get_stranger_info")
	// // 构建请求
	// req := map[string]interface{}{
	// 	"action": "get_stranger_info",
	// 	"params": map[string]interface{}{
	// 		"user_id":  uid,
	// 		"no_cache": false,
	// 	},
	// 	"echo": echo,
	// }
	// // 创建响应通道并添加到映射中
	// respChan := make(chan *ResponseGetStrangerInfo)
	// c.responseStrangerInfo[echo] = respChan
	// // 发送请求
	// data, _ := json.Marshal(req)
	// c.sendToWebSocketClient(c.ws, data)
	// // 等待响应或超时
	// select {
	// case resp := <-respChan:
	// 	delete(c.responseStrangerInfo, echo)
	// 	if resp.Retcode == 0 {
	// 		logger.Debug("GetStrangerInfo Success")
	// 		return *resp, nil
	// 	} else {
	// 		return ResponseGetStrangerInfo{}, errors.New(resp.Message)
	// 	}
	// case <-time.After(10 * time.Second):
	// 	delete(c.responseStrangerInfo, echo)
	// 	return ResponseGetStrangerInfo{}, errors.New("get stranger info timeout")
	// }
}
