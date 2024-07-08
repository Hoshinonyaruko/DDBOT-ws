package bilibili

import (
	"fmt"
	"github.com/Sora233/DDBOT/proxy_pool"
	"github.com/Sora233/DDBOT/requests"
	"github.com/Sora233/DDBOT/utils"
	"go.uber.org/atomic"
	"io"
	"net/http/cookiejar"
	"time"
)

const (
	PathXSpaceAccInfo = "/x/space/wbi/acc/info"
)

type XSpaceAccInfoRequest struct {
	Mid         int64  `json:"mid"`
	Platform    string `json:"platform"`
	Token       string `json:"token"`
	WebLocation string `json:"web_location"`
}

var cj atomic.Pointer[cookiejar.Jar]

func refreshCookieJar() {
	j, _ := cookiejar.New(nil)
	err := requests.Get("https://bilibili.com", nil, io.Discard,
		requests.WithCookieJar(j),
		AddUAOption(),
		requests.RequestAutoHostOption(),
		requests.HeaderOption("accept", "application/json"),
		requests.HeaderOption("accept-language", "zh-CN,zh;q=0.9"),
	)
	if err != nil {
		logger.Errorf("bilibili: refreshCookieJar request error %v", err)
	}
	cj.Store(j)
}

func XSpaceAccInfo(mid int64) (*XSpaceAccInfoResponse, error) {
	st := time.Now()
	defer func() {
		ed := time.Now()
		logger.WithField("FuncName", utils.FuncName()).Tracef("cost %v", ed.Sub(st))
	}()
	url := BPath(PathXSpaceAccInfo)
	params, err := utils.ToDatas(&XSpaceAccInfoRequest{
		Mid:         mid,
		Platform:    "web",
		WebLocation: "1550101",
	})
	if err != nil {
		return nil, err
	}
	signWbi(params)
	var opts = []requests.Option{
		requests.ProxyOption(proxy_pool.PreferNone),
		requests.TimeoutOption(time.Second * 15),
		AddUAOption(),
		requests.HeaderOption("Accept", "application/json"),
		requests.HeaderOption("Accept-language", "zh-CN,zh;q=0.9"),
		requests.HeaderOption("Origin", "https://space.bilibili.com"),
		requests.HeaderOption("Referer", fmt.Sprintf("https://space.bilibili.com/%v", mid)),
		requests.HeaderOption("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
		requests.CookieOption("buvid3", "55F2E775-090F-EC53-DDED-6FDD87687DAA76234infoc"),
		requests.CookieOption("b_nut", "1716350676"),
		requests.CookieOption("b_lsid", "5641F797_18F9E78EF04"),
		requests.CookieOption("_uuid", "F1BD8FB10-9651-10596-DA37-1CEC9A469A5376746infoc"),
		requests.RequestAutoHostOption(),
		requests.WithCookieJar(cj.Load()),
		requests.NotIgnoreEmptyOption(),
		delete412ProxyOption,
	}
	opts = append(opts, GetVerifyOption()...)
	xsai := new(XSpaceAccInfoResponse)
	err = requests.Get(url, params, xsai, opts...)
	if err != nil {
		return nil, err
	}
	return xsai, nil
}
