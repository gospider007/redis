package redis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"sync"

	"github.com/gospider007/gson"
	"github.com/gospider007/gtls"
	"github.com/gospider007/requests"
	"github.com/gospider007/tools"
	"github.com/redis/go-redis/v9"
	"golang.org/x/exp/slices"
)

type Client struct {
	object *redis.Client
	proxys map[string][]string
	lock   sync.Mutex
	ctx    context.Context
}
type ClientOption struct {
	Addr        string // 地址
	Pwd         string //密码
	Db          int    //数据库
	Socks5Proxy string // socks5 proxy
}
type redisDialer struct {
	dialer *requests.Dialer
	proxy  *requests.Address
}

func (obj *redisDialer) Dialer(ctx context.Context, network string, addr string) (net.Conn, error) {
	remoteAddrress, err := requests.GetAddressWithAddr(addr)
	if err != nil {
		return nil, err
	}
	if obj.proxy != nil {
		return obj.dialer.Socks5TcpProxy(requests.NewResponse(ctx, requests.RequestOption{}), *obj.proxy, remoteAddrress)
	}
	return obj.dialer.DialContext(requests.NewResponse(ctx, requests.RequestOption{}), network, remoteAddrress)
}
func NewClient(ctx context.Context, option ClientOption) (*Client, error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	if option.Addr == "" {
		option.Addr = ":6379"
	}
	redisDia := &redisDialer{
		dialer: &requests.Dialer{},
	}
	if option.Socks5Proxy != "" {
		proxy, err := gtls.VerifyProxy(option.Socks5Proxy)
		if err != nil {
			return nil, err
		}
		proxyAddress, err := requests.GetAddressWithUrl(proxy)
		if err != nil {
			return nil, err
		}
		if proxyAddress.Scheme != "socks5" {
			return nil, fmt.Errorf("only support socks5 proxy")
		}
		redisDia.proxy = &proxyAddress
	}
	redCli := redis.NewClient(&redis.Options{
		Addr:     option.Addr,
		DB:       option.Db,
		Password: option.Pwd,
		Dialer:   redisDia.Dialer,
	})
	_, err := redCli.Ping(ctx).Result()
	return &Client{ctx: ctx, object: redCli, proxys: make(map[string][]string)}, err
}

// 集合增加元素
func (r *Client) SAdd(ctx context.Context, name string, vals ...any) (int64, error) {
	if ctx == nil {
		ctx = r.ctx
	}
	return r.object.SAdd(ctx, name, vals...).Result()
}

// Redis 会员键命令输出作为slince
func (r *Client) SMembers(ctx context.Context, key string) ([]string, error) {
	if ctx == nil {
		ctx = r.ctx
	}
	return r.object.SMembers(ctx, key).Result()
}

// Redis 会员键命令输出作为map
func (r *Client) SMembersMap(ctx context.Context, key string) (map[string]struct{}, error) {
	if ctx == nil {
		ctx = r.ctx
	}
	return r.object.SMembersMap(ctx, key).Result()
}

// 判断元素是否存在集合
func (r *Client) SExists(ctx context.Context, name string, val any) (bool, error) {
	if ctx == nil {
		ctx = r.ctx
	}
	return r.object.SIsMember(ctx, name, val).Result()
}

// 集合长度
func (r *Client) SLen(ctx context.Context, name string) (int64, error) {
	if ctx == nil {
		ctx = r.ctx
	}
	return r.object.SCard(ctx, name).Result()
}

// 集合所有的值
func (r *Client) SVals(ctx context.Context, name string) ([]string, error) {
	if ctx == nil {
		ctx = r.ctx
	}
	return r.object.SMembers(ctx, name).Result()
}

// 删除一个元素返回
func (r *Client) SPop(ctx context.Context, name string) (string, error) {
	if ctx == nil {
		ctx = r.ctx
	}
	return r.object.SPop(ctx, name).Result()
}

// 删除元素
func (r *Client) SRem(ctx context.Context, name string, vals ...any) (int64, error) {
	if ctx == nil {
		ctx = r.ctx
	}
	return r.object.SRem(ctx, name, vals...).Result()
}

// 获取字典中的key值
func (r *Client) HGet(ctx context.Context, name string, key string) (string, error) {
	if ctx == nil {
		ctx = r.ctx
	}
	return r.object.HGet(ctx, name, key).Result()
}

// 获取字典
func (r *Client) HAll(ctx context.Context, name string) (map[string]string, error) {
	if ctx == nil {
		ctx = r.ctx
	}
	return r.object.HGetAll(ctx, name).Result()
}

// 获取字典所有key
func (r *Client) HKeys(ctx context.Context, name string) ([]string, error) {
	if ctx == nil {
		ctx = r.ctx
	}
	return r.object.HKeys(ctx, name).Result()
}

// 获取字典所有值
func (r *Client) HVals(ctx context.Context, name string) ([]string, error) {
	if ctx == nil {
		ctx = r.ctx
	}
	return r.object.HVals(ctx, name).Result()
}

// 获取字典长度
func (r *Client) HLen(ctx context.Context, name string) (int64, error) {
	if ctx == nil {
		ctx = r.ctx
	}
	return r.object.HLen(ctx, name).Result()
}

// 设置字典的值
func (r *Client) HSet(ctx context.Context, name string, key string, val string) (int64, error) {
	if ctx == nil {
		ctx = r.ctx
	}
	return r.object.HSet(ctx, name, key, val).Result()
}

// 删除字典的值
func (r *Client) HDel(ctx context.Context, name string, key string) (int64, error) {
	if ctx == nil {
		ctx = r.ctx
	}
	return r.object.HDel(ctx, name, key).Result()
}

// 关闭客户端
func (r *Client) Close() error {
	return r.object.Close()
}

// 代理操作
// 获取最新代理
type Proxy struct {
	Ip    string
	Port  int64
	Ttl   int64
	Usr   string
	Pwd   string
	Proxy string
}

func (r *Client) GetProxy(ctx context.Context, key string) (string, error) {
	if ctx == nil {
		ctx = r.ctx
	}
	vals, err := r.GetProxys(ctx, key)
	if err != nil {
		return "", err
	}
	return vals[0], nil
}
func (r *Client) GetRandProxy(ctx context.Context, key string) (string, error) {
	if ctx == nil {
		ctx = r.ctx
	}
	vals, err := r.GetProxys(ctx, key)
	if err != nil {
		return "", err
	}
	return vals[tools.RanInt(0, len(vals))], nil
}

// 获取所有代理
func (r *Client) GetProxys(ctx context.Context, key string) ([]string, error) {
	if ctx == nil {
		ctx = r.ctx
	}
	proxys, err := r.GetProxyDatas(ctx, key)
	if err != nil {
		return nil, err
	}
	results := []string{}
	for _, proxy := range proxys {
		results = append(results, proxy.Proxy)
	}
	return results, nil
}

// 获取所有代理
func (r *Client) GetProxyDatas(ctx context.Context, key string) ([]Proxy, error) {
	if ctx == nil {
		ctx = r.ctx
	}
	vals, err := r.HVals(ctx, key)
	if err != nil {
		return nil, err
	}
	valLen := len(vals)
	if valLen == 0 {
		return nil, errors.New("代理为空")
	}
	proxys := []Proxy{}
	for _, jsonStr := range vals {
		val, err := gson.Decode(jsonStr)
		if err != nil {
			return nil, err
		}
		var proxy Proxy
		proxy.Ip = val.Get("ip").String()
		if proxy.Ip = val.Get("ip").String(); proxy.Ip == "" {
			continue
		}
		if proxy.Port = val.Get("port").Int(); proxy.Port == 0 {
			continue
		}

		proxy.Usr = val.Get("usr").String()
		proxy.Pwd = val.Get("pwd").String()
		proxy.Ttl = val.Get("ttl").Int()

		if proxy.Usr != "" && proxy.Pwd != "" {
			proxy.Proxy = fmt.Sprintf("%s:%s@%s", proxy.Usr, proxy.Pwd, net.JoinHostPort(proxy.Ip, strconv.Itoa(int(proxy.Port))))
		} else {
			proxy.Proxy = net.JoinHostPort(proxy.Ip, strconv.Itoa(int(proxy.Port)))
		}
		proxys = append(proxys, proxy)
	}
	sort.SliceStable(proxys, func(i, j int) bool {
		return proxys[i].Ttl > proxys[j].Ttl
	})
	return proxys, nil
}

// 获取所有代理,排序后的
func (r *Client) GetOrderProxys(ctx context.Context, key string) ([]string, error) {
	if ctx == nil {
		ctx = r.ctx
	}
	proxys, err := r.GetProxys(ctx, key)
	if err != nil {
		return proxys, err
	}
	total := len(proxys)
	results := make([]string, total)
	orderProxy, ok := r.proxys[key]
	if !ok {
		r.proxys[key] = proxys
		return proxys, nil
	}
	newProxys := []string{}
	for _, val := range proxys {
		index := slices.Index(orderProxy, val)
		if index < total && index != -1 {
			results[index] = val
		} else {
			newProxys = append(newProxys, val)
		}
	}
	j := 0
	for i, reslut := range results {
		if reslut == "" {
			results[i] = newProxys[j]
			j++
		}
	}
	r.lock.Lock()
	r.proxys[key] = results
	r.lock.Unlock()
	return results, nil
}
