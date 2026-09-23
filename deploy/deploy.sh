#!/usr/bin/env bash
# ============================================================
# 燕子户外 · 一键部署脚本（Ubuntu / Debian）
# 用法：sudo bash deploy/deploy.sh
# 脚本会：装环境 → 拉代码 → 填密钥 → 守护代理 → 配 Nginx → 自检
# ============================================================
set -e

echo "=============================================="
echo "  燕子户外 · 一键部署"
echo "=============================================="

# 1. 必须 root
if [ "$(id -u)" != "0" ]; then
  echo "请用 root 运行：sudo bash deploy/deploy.sh"
  exit 1
fi

# 2. 收集信息（Key 不会被回显到屏幕之外）
read -p "这台服务器的公网 IP（如 1.2.3.4）：" SRV_IP
read -p "DeepSeek API Key：" DS_KEY
read -p "聚合数据 Key（没有就回车跳过）：" JUHE_KEY
read -p "和风天气 Key：" QW_KEY

# 简单校验 IP
if ! echo "$SRV_IP" | grep -Eq '^[0-9]{1,3}(\.[0-9]{1,3}){3}$'; then
  echo "IP 格式不对，请重新运行。"
  exit 1
fi

# 3. 装环境
echo ">>> [1/6] 安装依赖（python3 / nginx / git / curl）..."
apt-get update -y
apt-get install -y python3 nginx git curl

# 4. 拉代码
APP_DIR=/var/www/yanzi-outdoor-agent
if [ -d "$APP_DIR/.git" ]; then
  echo ">>> [2/6] 已存在项目，拉取最新代码..."
  cd "$APP_DIR" && git pull
else
  echo ">>> [2/6] 克隆代码（若卡住说明连不上 GitHub，见文末说明）..."
  rm -rf "$APP_DIR"
  git clone https://github.com/zsrzyy/yanzi-outdoor-agent.git "$APP_DIR"
  cd "$APP_DIR"
fi

# 5. 写密钥
echo ">>> [3/6] 写入密钥文件..."
printf '%s\n' "$DS_KEY" > config.txt
if [ -n "$JUHE_KEY" ]; then printf '%s\n' "$JUHE_KEY" > juhe_key.txt; fi
printf '%s\n' "$QW_KEY" > qweather_key.txt
chmod 600 config.txt juhe_key.txt qweather_key.txt 2>/dev/null || true

# 6. 守护代理
echo ">>> [4/6] 启动代理守护服务..."
cp deploy/yanzi-proxy.service /etc/systemd/system/yanzi-proxy.service
systemctl daemon-reload
systemctl enable --now yanzi-proxy
sleep 2
if systemctl is-active --quiet yanzi-proxy; then
  echo "    代理已启动 ✅"
else
  echo "    ⚠️ 代理启动失败，请运行：journalctl -u yanzi-proxy -n 30 查看原因"
fi

# 7. Nginx
echo ">>> [5/6] 配置 Nginx（80 端口）..."
sed "s/101\.43\.127\.252/$SRV_IP/g" nginx-ip.conf > /etc/nginx/conf.d/yanzi-outdoor.conf
nginx -t
systemctl reload nginx

# 8. 自检
echo ">>> [6/6] 自检..."
echo -n "    本地代理 health："
curl -s http://127.0.0.1:8899/health | head -c 120; echo
echo -n "    首页 HTTP 状态："
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1/

echo "=============================================="
echo "  ✅ 部署完成！"
echo "  浏览器访问：http://$SRV_IP"
echo "  若打不开 → 腾讯云「防火墙」放行 TCP 80 端口"
echo "=============================================="
