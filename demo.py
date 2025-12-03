import re
import pandas as pd

def clean_news_text(text):
    """清洗新闻正文：去除无意义标识、免责声明、多余空行、英文引号等"""
    # 1. 去除英文引号（" 和 '）
    text = re.sub(r'["\']', '', text)
    # 2. 去除通讯社标识（保留地点日期，只删大写无意义代码）
    text = re.sub(r'\s+[A-Z]+\s+[A-Z]+(?:\s+[A-Z]+)*\s+', ' ', text)
    # 3. 去除免责声明（Disclaimer 及后续内容）
    text = re.sub(r'Disclaimer :-.*$', '', text, flags=re.DOTALL)
    # 4. 去除图片来源标注
    text = re.sub(r'Pool photo by .*?/', '', text)
    # 5. 去除多余空格和特殊字符
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def split_valid_sentences(cleaned_text):
    """精准拆分英文句子，确保每个元素是单个句子且以.结尾，最终返回相邻句子对（\t分隔）"""
    if not cleaned_text:
        return []
    
    # 核心分割规则：匹配句末标点（. ! ?）+ 空格 + 大写字母（下一句开头）
    split_marker = '<SPLIT>'
    text_with_markers = re.sub(r'(?<=[.!?])\s*', f' {split_marker} ', cleaned_text)
    
    # 拆分后过滤无效内容
    raw_sentences = [s.strip() for s in text_with_markers.split(split_marker)]
    valid_sentences = []
    
    for sent in raw_sentences:
        # 过滤条件：长度≥8（排除过短句子）、含英文单词、不是纯数字/标点
        if (len(sent) >= 8 
            and re.search(r'\b[a-zA-Z]{2,}\b', sent)
            and not re.fullmatch(r'[0-9\s.,;:!?-]+', sent)):
            
            # 强制以 . 结尾（统一格式）
            if sent.endswith(('!', '?')):
                sent = sent[:-1] + '.'
            elif not sent.endswith('.'):
                sent = sent + '.'
            
            valid_sentences.append(sent)
    
    # 新增：将相邻两个句子用\t连接成句子对（0-1, 2-3, 4-5...）
    sentence_pairs = []
    # 步长为2，循环配对相邻句子
    for i in range(0, len(valid_sentences) - 1, 2):
        pair = f"{valid_sentences[i]}\t{valid_sentences[i+1]}"
        sentence_pairs.append(pair)
    
    # 返回句子对数组（每个元素是一个\t分隔的句子对）
    return sentence_pairs

def extract_single_sentences_from_samples(samples):
    """提取所有有效句子，每行一个句子"""
    result = []
    for sample in samples:
        raw_text = sample[1] if len(sample) > 1 else ""
        cleaned_text = clean_news_text(raw_text)
        if not cleaned_text:
            continue
        
        # 拆分并获取单个有效句子
        single_sentences = split_valid_sentences(cleaned_text)
        result.extend(single_sentences)  # 直接添加所有单个句子
    
    return result

# ------------------- 执行处理 -------------------
file_path = "webtext2-00027-of-00027.parquet"
df = pd.read_parquet(file_path)
data_list = df.values.tolist()
print("数据读取完成，开始处理...")
print("-" * 80)

# 测试前3个样本（可改为 raw_data = data_list 处理全部）
raw_data = data_list[1000:1100]
processed_sentences = extract_single_sentences_from_samples(raw_data)

# 输出验证（每行一个句子）
print("处理后的句子（每行一个，均以.结尾）：")
print("-" * 80)

# 保存文件（每行一个句子）
output_file = "test.txt"
with open(output_file, "w", encoding="utf-8") as f:
    for sentence in processed_sentences:
        f.write(sentence + "\n")  # 每个句子单独一行

print(f"处理完成！共生成 {len(processed_sentences)} 个句子")
print(f"结果已保存到：{output_file}")