import time
import uuid
import asyncio
from typing import Dict, List, Optional
from langgraph.func import entrypoint, task
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import interrupt, Command, RetryPolicy, CachePolicy


# 1. 定义任务：用户意图识别
@task(name="intent_classifier")
def classify_intent(user_message: str) -> Dict[str, float]:
    """识别用户意图"""
    # 模拟AI模型推理
    time.sleep(0.5)

    # 简单的意图分类逻辑
    if "价格" in user_message or "多少钱" in user_message:
        return {"pricing": 0.9, "support": 0.1}
    elif "问题" in user_message or "帮助" in user_message:
        return {"support": 0.8, "pricing": 0.2}
    else:
        return {"general": 0.7, "support": 0.3}


# 2. 定义任务：获取产品信息
@task(
    name="product_info",
    retry_policy=RetryPolicy(max_attempts=3, initial_interval=1.0),  # 正确的重试策略
)
def get_product_info(product_name: str) -> Dict[str, str]:
    """获取产品详细信息"""
    # 模拟API调用
    time.sleep(1.0)

    products = {
        "手机": {"价格": "¥3999", "库存": "充足", "描述": "最新款智能手机"},
        "电脑": {"价格": "¥6999", "库存": "有限", "描述": "高性能笔记本电脑"},
        "平板": {"价格": "¥2999", "库存": "充足", "描述": "便携式平板电脑"},
    }

    return products.get(product_name, {"错误": "产品不存在"})


# 3. 定义任务：技术支持
@task(name="tech_support")
async def provide_tech_support(issue: str) -> str:
    """提供技术支持"""
    # 模拟异步API调用
    await asyncio.sleep(0.8)

    solutions = {
        "无法开机": "请检查电源连接，尝试长按电源键10秒",
        "网络问题": "请重启路由器，检查网络设置",
        "软件问题": "请尝试更新到最新版本或重新安装",
    }

    return solutions.get(issue, "请描述更具体的问题，我们将为您提供帮助")


# 4. 定义任务：生成回复
@task(name="response_generator", cache_policy=CachePolicy(ttl=300))  # 正确的缓存策略
def generate_response(intent: Dict, context: Dict) -> str:
    """根据意图和上下文生成回复"""
    time.sleep(0.3)

    if intent.get("pricing", 0) > 0.5:
        product = context.get("product", "未知产品")
        info = context.get("product_info", {})
        price = info.get("价格", "暂无价格信息")
        return f"关于{product}的价格：{price}"

    elif intent.get("support", 0) > 0.5:
        issue = context.get("issue", "一般问题")
        solution = context.get("solution", "我们将尽快为您解决")
        return f"针对您的问题'{issue}'，建议：{solution}"

    else:
        return "您好！我是智能客服，请问有什么可以帮助您的吗？"


# 5. 定义主工作流
@entrypoint(checkpointer=InMemorySaver())
def customer_service_workflow(user_input: Dict[str, str]) -> Dict[str, str]:
    """
    智能客服工作流

    Args:
        user_input: 用户输入，包含message和可选的其他信息

    Returns:
        客服回复和相关信息
    """
    message = user_input.get("message", "")
    product = user_input.get("product", "")

    # 并行执行意图识别和产品信息获取
    intent_future = classify_intent(message)

    if product:
        product_info_future = get_product_info(product)
    else:
        product_info_future = None

    # 等待意图识别结果
    intent_result = intent_future.result()

    # 根据意图决定下一步
    context = {}

    if intent_result.get("pricing", 0) > 0.5 and product:
        if product_info_future:
            product_info = product_info_future.result()
            context.update({"product": product, "product_info": product_info})

    elif intent_result.get("support", 0) > 0.5:
        # 需要人工介入的情况
        human_review = interrupt(
            {
                "question": f"用户问题：{message}",
                "intent": intent_result,
                "type": "technical_support",
            }
        )
        context.update({"issue": message, "solution": human_review})

    # 生成最终回复
    response_future = generate_response(intent_result, context)
    final_response = response_future.result()

    return {
        "response": final_response,
        "intent": intent_result,
        "context": context,
        "timestamp": str(time.time()),
    }


# 6. 使用示例
async def main():
    # 创建配置
    config = {"configurable": {"thread_id": str(uuid.uuid4())}}

    # 测试用例1：价格查询
    print("=== 测试1：价格查询 ===")
    result1 = customer_service_workflow.invoke(
        {"message": "手机多少钱？", "product": "手机"}, config=config
    )
    print(f"回复：{result1['response']}")
    print(f"识别意图：{result1['intent']}")

    # 测试用例2：技术支持（需要人工介入）
    print("\n=== 测试2：技术支持 ===")
    try:
        # 第一次调用会中断等待人工介入
        for chunk in customer_service_workflow.stream(
            {"message": "我的电脑无法开机了", "product": "电脑"}, config=config
        ):
            print(f"流式输出：{chunk}")
    except Exception as e:
        print(f"工作流中断，等待人工介入: {e}")

    # 模拟人工回复后继续
    human_response = "请检查电源线和电池连接"
    for chunk in customer_service_workflow.stream(
        Command(resume=human_response), config=config
    ):
        print(f"继续执行：{chunk}")

    # 测试用例3：异步调用
    print("\n=== 测试3：异步调用 ===")
    result3 = await customer_service_workflow.ainvoke(
        {"message": "网络连接有问题", "product": "平板"}, config=config
    )
    print(f"异步回复：{result3['response']}")


# 7. 批量处理示例
@entrypoint()
def batch_customer_service(messages: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """批量处理客户消息"""
    futures = []

    for msg in messages:
        # 为每个消息创建独立的工作流调用
        future = customer_service_workflow.invoke(msg)
        futures.append(future)

    # 等待所有任务完成
    results = [f.result() for f in futures]
    return results


if __name__ == "__main__":
    asyncio.run(main())
