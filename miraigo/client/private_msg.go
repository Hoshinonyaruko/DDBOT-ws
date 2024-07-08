package client

import (
	"encoding/json"
	"math"
	"math/rand"
	"regexp"
	"strconv"
	"time"

	"github.com/pkg/errors"

	"github.com/Mrs4s/MiraiGo/client/pb/msg"
	"github.com/Mrs4s/MiraiGo/internal/proto"
	"github.com/Mrs4s/MiraiGo/message"
)

type WebSocketActionMessagePrivate struct {
	Action string                 `json:"action"`
	Params WebSocketParamsPrivate `json:"params"`
	Echo   string                 `json:"echo,omitempty"`
}

type WebSocketParamsPrivate struct {
	UserID  string `json:"user_id"`
	Message string `json:"message"`
}

// 发送私聊信息
func (c *QQClient) SendPrivateMessage(target int64, m *message.SendingMessage, newstr string) *message.PrivateMessage {
	// 检查target是否是由字符串经MD5得到的
	originalUserID, exists := originalStringFromInt64(target)

	// 如果存在于映射中，则使用原始字符串，否则使用当前的target值
	finalUserID := strconv.FormatInt(target, 10)
	if exists {
		finalUserID = originalUserID
	}

	// 图片数量统计
	imgCount := 0
	for _, e := range m.Elements {
		switch e.Type() {
		case message.Image:
			imgCount++
		}
	}
	//判定消息长度限制
	msgLen := message.EstimateLength(m.Elements)
	imgLen := 0
	totalLen := 0
	if len(m.Elements) > 0 {
		for _, e := range m.Elements {
			//判断消息是否为图片
			if text, ok := e.(*message.TextElement); ok {
				re := regexp.MustCompile(`\[CQ:image,file=(.*?)\]`)
				picCount := re.FindAllStringIndex(text.Content, -1)
				imgCount += len(picCount)
				if imgCount > 0 {
					tmpLen := 0
					for _, pic := range picCount {
						tmpLen += pic[1] - pic[0]
					}
					imgLen += tmpLen
					msgLen -= tmpLen
				}
			}
		}
		totalLen = msgLen + imgLen
		logger.Infof("本次发送总长: %d, 文本: %d, 图片: %d, 长度：%d", totalLen, msgLen, imgCount, imgLen)
	}
	//判断是否超过最大发送长度
	if msgLen > message.MaxMessageSize || imgCount > 20 {
		return nil
	}
	// 构造消息
	// echo := generateEcho("send_private_msg")
	// msg := WebSocketActionMessagePrivate{
	// 	Action: "send_private_msg",
	// 	Params: WebSocketParamsPrivate{
	// 		UserID:  finalUserID,
	// 		Message: newstr,
	// 	},
	// 	Echo: echo,
	// }

	// data, err := json.Marshal(msg)
	// if err != nil {
	// 	//fmt.Printf("Failed to marshal message to JSON: %v", err)
	// 	logger.Errorf("Failed to marshal message to JSON: %v", err)
	// 	return nil
	// }

	// respChan := make(chan *ResponseSendMessage)
	// c.responseMessage[echo] = respChan
	expTime := math.Ceil(float64(totalLen) / 131072)
	if totalLen < 1024 && imgCount > 0 {
		expTime = 90
	}
	expTime += 6
	tmpMsg := ""
	if len(newstr) > 75 {
		tmpMsg = newstr[:75] + "..."
	} else {
		tmpMsg = newstr
	}
	logger.Infof("发送 私聊消息 给 (%v) 预期耗时 %.0fs: %s", finalUserID, expTime, tmpMsg)
	// c.sendToWebSocketClient(c.ws, data)

	data, err := c.SendApi("send_private_msg", map[string]any{
		"user_id": finalUserID,
		"message": newstr,
	}, expTime)
	if err != nil {
		logger.Errorf("发送私聊消息失败: %v", err)
		return nil
	}
	t, err := json.Marshal(data)
	if err != nil {
		logger.Errorf("解析私聊消息响应失败: %v", err)
		return nil
	}
	var resp ResponseSendMessage
	if err = json.Unmarshal(t, &resp.Data); err != nil {
		logger.Errorf("解析私聊消息响应失败: %v", err)
		return nil
	}
	c.stat.MessageSent.Add(1)
	retMsg := &message.PrivateMessage{
		Id:         resp.Data.MessageID,
		InternalId: int32(rand.Uint32()),
		Self:       c.Uin,
		Target:     target,
		Sender: &message.Sender{
			Uin:      c.Uin,
			Nickname: c.Nickname,
			IsFriend: true,
		},
		Time:     int32(time.Now().Unix()),
		Elements: m.Elements,
	}
	go c.SelfPrivateMessageEvent.dispatch(c, retMsg)
	return retMsg
	// select {
	// case resp := <-respChan:
	// 	delete(c.responseMessage, echo)
	// 	if resp.Retcode == 0 {
	// 		c.stat.MessageSent.Add(1)
	// 		retMsg := &message.PrivateMessage{
	// 			Id:         resp.Data.MessageID,
	// 			InternalId: int32(rand.Uint32()),
	// 			Self:       c.Uin,
	// 			Target:     target,
	// 			Sender: &message.Sender{
	// 				Uin:      c.Uin,
	// 				Nickname: c.Nickname,
	// 				IsFriend: true,
	// 			},
	// 			Time:     int32(time.Now().Unix()),
	// 			Elements: m.Elements,
	// 		}
	// 		go c.SelfPrivateMessageEvent.dispatch(c, retMsg)
	// 		return retMsg
	// 	} else {
	// 		logger.Errorf("发送私聊消息失败: %d", resp.Retcode)
	// 	}
	// 	//return c.sendGroupMessage(groupCode, false, m, msgID)
	// case <-time.After(time.Duration(expTime) * time.Second):
	// 	delete(c.responseMessage, echo)
	// 	logger.Errorf("发送私聊消息超时: %d", expTime)
	// }
	// return nil
	// mr := int32(rand.Uint32())
	// var seq int32
	// t := time.Now().Unix()
	// imgCount := 0
	// frag := true
	// for _, e := range m.Elements {
	// 	switch e.Type() {
	// 	case message.Image:
	// 		imgCount++
	// 	case message.Reply:
	// 		frag = false
	// 	}
	// }
	// msgLen := message.EstimateLength(m.Elements)
	// if msgLen > message.MaxMessageSize || imgCount > 50 {
	// 	return nil
	// }
	// if frag && (msgLen > 300 || imgCount > 2) && target == c.Uin {
	// 	div := int32(rand.Uint32())
	// 	fragmented := m.ToFragmented()
	// 	for i, elems := range fragmented {
	// 		fseq := c.nextFriendSeq()
	// 		if i == 0 {
	// 			seq = fseq
	// 		}
	// 		_, pkt := c.buildFriendSendingPacket(target, fseq, mr, int32(len(fragmented)), int32(i), div, t, elems)
	// 		_ = c.sendPacket(pkt)
	// 	}
	// } else {
	// 	seq = c.nextFriendSeq()
	// 	if target != c.Uin {
	// 		_, pkt := c.buildFriendSendingPacket(target, seq, mr, 1, 0, 0, t, m.Elements)
	// 		_ = c.sendPacket(pkt)
	// 	}
	// }
	// c.stat.MessageSent.Add(1)
	// ret := &message.PrivateMessage{
	// 	Id:         seq,
	// 	InternalId: mr,
	// 	Self:       c.Uin,
	// 	Target:     target,
	// 	Time:       int32(t),
	// 	Sender: &message.Sender{
	// 		Uin:      c.Uin,
	// 		Nickname: c.Nickname,
	// 		IsFriend: true,
	// 	},
	// 	Elements: m.Elements,
	// }
}

func (c *QQClient) SendGroupTempMessage(groupCode, target int64, m *message.SendingMessage) *message.TempMessage {
	group := c.FindGroup(groupCode)
	if group == nil {
		return nil
	}
	//群临时
	// if c.FindFriend(target) != nil {
	// 	pm := c.SendPrivateMessage(target, m)
	// 	return &message.TempMessage{
	// 		Id:        pm.Id,
	// 		GroupCode: group.Code,
	// 		GroupName: group.Name,
	// 		Self:      c.Uin,
	// 		Sender:    pm.Sender,
	// 		Elements:  m.Elements,
	// 	}
	// }
	mr := int32(rand.Uint32())
	seq := c.nextFriendSeq()
	t := time.Now().Unix()
	_, pkt := c.buildGroupTempSendingPacket(group.Uin, target, seq, mr, t, m)
	_ = c.sendPacket(pkt)
	c.stat.MessageSent.Add(1)
	return &message.TempMessage{
		Id:        seq,
		GroupCode: group.Code,
		GroupName: group.Name,
		Self:      c.Uin,
		Sender: &message.Sender{
			Uin:      c.Uin,
			Nickname: c.Nickname,
			IsFriend: true,
		},
		Elements: m.Elements,
	}
}

func (c *QQClient) sendWPATempMessage(target int64, sig []byte, m *message.SendingMessage) *message.TempMessage {
	mr := int32(rand.Uint32())
	seq := c.nextFriendSeq()
	t := time.Now().Unix()
	_, pkt := c.buildWPATempSendingPacket(target, sig, seq, mr, t, m)
	_ = c.sendPacket(pkt)
	c.stat.MessageSent.Add(1)
	return &message.TempMessage{
		Id:   seq,
		Self: c.Uin,
		Sender: &message.Sender{
			Uin:      c.Uin,
			Nickname: c.Nickname,
			IsFriend: true,
		},
		Elements: m.Elements,
	}
}

func (s *TempSessionInfo) SendMessage(m *message.SendingMessage) (*message.TempMessage, error) {
	switch s.Source {
	case GroupSource:
		return s.client.SendGroupTempMessage(s.GroupCode, s.Sender, m), nil
	case ConsultingSource:
		return s.client.sendWPATempMessage(s.Sender, s.sig, m), nil
	default:
		return nil, errors.New("unsupported message source")
	}
}

/* this function is unused
func (c *QQClient) buildGetOneDayRoamMsgRequest(target, lastMsgTime, random int64, count uint32) (uint16, []byte) {
	seq := c.nextSeq()
	req := &msg.PbGetOneDayRoamMsgReq{
		PeerUin:     proto.Uint64(uint64(target)),
		LastMsgTime: proto.Uint64(uint64(lastMsgTime)),
		Random:      proto.Uint64(uint64(random)),
		ReadCnt:     proto.Some(count),
	}
	payload, _ := proto.Marshal(req)
	packet := packets.BuildUniPacket(c.Uin, seq, "MessageSvc.PbGetOneDayRoamMsg", 1, c.SessionId, EmptyBytes, c.sigInfo.d2Key, payload)
	return seq, packet
}
*/

// MessageSvc.PbSendMsg
func (c *QQClient) buildFriendSendingPacket(target int64, msgSeq, r, pkgNum, pkgIndex, pkgDiv int32, time int64, m []message.IMessageElement) (uint16, []byte) {
	var ptt *msg.Ptt
	if len(m) > 0 {
		if p, ok := m[0].(*message.PrivateVoiceElement); ok {
			ptt = p.Ptt
			m = []message.IMessageElement{}
		}
	}
	req := &msg.SendMessageRequest{
		RoutingHead: &msg.RoutingHead{C2C: &msg.C2C{ToUin: proto.Some(target)}},
		ContentHead: &msg.ContentHead{PkgNum: proto.Some(pkgNum), PkgIndex: proto.Some(pkgIndex), DivSeq: proto.Some(pkgDiv)},
		MsgBody: &msg.MessageBody{
			RichText: &msg.RichText{
				Elems: message.ToProtoElems(m, false),
				Ptt:   ptt,
			},
		},
		MsgSeq:     proto.Some(msgSeq),
		MsgRand:    proto.Some(r),
		SyncCookie: syncCookie(time),
	}
	payload, _ := proto.Marshal(req)
	return c.uniPacket("MessageSvc.PbSendMsg", payload)
}

// MessageSvc.PbSendMsg
func (c *QQClient) buildGroupTempSendingPacket(groupUin, target int64, msgSeq, r int32, time int64, m *message.SendingMessage) (uint16, []byte) {
	req := &msg.SendMessageRequest{
		RoutingHead: &msg.RoutingHead{GrpTmp: &msg.GrpTmp{
			GroupUin: proto.Some(groupUin),
			ToUin:    proto.Some(target),
		}},
		ContentHead: &msg.ContentHead{PkgNum: proto.Int32(1)},
		MsgBody: &msg.MessageBody{
			RichText: &msg.RichText{
				Elems: message.ToProtoElems(m.Elements, false),
			},
		},
		MsgSeq:     proto.Some(msgSeq),
		MsgRand:    proto.Some(r),
		SyncCookie: syncCookie(time),
	}
	payload, _ := proto.Marshal(req)
	return c.uniPacket("MessageSvc.PbSendMsg", payload)
}

func (c *QQClient) buildWPATempSendingPacket(uin int64, sig []byte, msgSeq, r int32, time int64, m *message.SendingMessage) (uint16, []byte) {
	req := &msg.SendMessageRequest{
		RoutingHead: &msg.RoutingHead{WpaTmp: &msg.WPATmp{
			ToUin: proto.Uint64(uint64(uin)),
			Sig:   sig,
		}},
		ContentHead: &msg.ContentHead{PkgNum: proto.Int32(1)},
		MsgBody: &msg.MessageBody{
			RichText: &msg.RichText{
				Elems: message.ToProtoElems(m.Elements, false),
			},
		},
		MsgSeq:     proto.Some(msgSeq),
		MsgRand:    proto.Some(r),
		SyncCookie: syncCookie(time),
	}
	payload, _ := proto.Marshal(req)
	return c.uniPacket("MessageSvc.PbSendMsg", payload)
}

func syncCookie(time int64) []byte {
	cookie, _ := proto.Marshal(&msg.SyncCookie{
		Time:   proto.Some(time),
		Ran1:   proto.Int64(rand.Int63()),
		Ran2:   proto.Int64(rand.Int63()),
		Const1: proto.Some(syncConst1),
		Const2: proto.Some(syncConst2),
		Const3: proto.Int64(0x1d),
	})
	return cookie
}
