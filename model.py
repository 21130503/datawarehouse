from click import prompt
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain  # Keeping LLMChain for compatibility
from langchain_community.llms.ctransformers import CTransformers


class Model():
    def __init__(self, dest):
        self.dest = dest

    def load_llms(self):
        llm = CTransformers(
            model=self.dest,
            model_type='llama',
            max_new_tokens=1024,
            temperature=0
        )
        return llm

    def create_prompts(self, template):
        prompts = PromptTemplate(template=template, input_variables=['question'])
        return prompts

    def summarize_prompts(self, llm, prompts):
        llm_chain = LLMChain(llm=llm, prompt=prompts)
        return llm_chain


model = Model('model/vinallama-7b-chat_q5_0.gguf')
template = """
<|im_start|>system
Bạn là một trợ lý AI hữu ích. Hãy phân tích văn bản sau và trả về thông tin về tai nạn giao thông theo định dạng bảng. Cụ thể, hãy cho biết:
- Số người chết
- Số người bị thương
- Mức độ nghiêm trọng (Nghiêm trọng nếu có người chết; rất nghiêm trọng nếu có trên 2 người chết)
- Địa điểm tai nạn

Nếu không có thông tin cho cột nào, hãy để trống.
Hãy đảm bảo rằng câu trả lời của bạn rõ ràng và dễ hiểu.
<|im_end|>
<|im_start|user:
Hãy cho biết số người chết, số người bị thương, mức độ nghiêm trọng, địa điểm tai nạn của văn bản sau {question}<|im_end|>
"""

prompt = model.create_prompts(template)
llm = model.load_llms()
chain = model.summarize_prompts(llm, prompt)

content = """
                Nỗi ân hận của người mẹ trong vụ đoàn đua tông chết cô gái dừng chờ đèn đỏ
            """

resp = chain.invoke({'question': content})

print(resp)
