from click import prompt
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain  # Keeping LLMChain for compatibility
from langchain_community.llms.ctransformers import CTransformers
import re

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
    def respone(self, question):
        template = """
        <|im_start|>system
        Bạn là một trợ lý AI hữu ích. Hãy phân tích văn bản sau và trả về thông tin về tai nạn giao thông theo định dạng bảng. Cụ thể, hãy cho biết:
        - Số người chết
        - Số người bị thương
        - Mức độ nghiêm trọng (Nghiêm trọng nếu có người chết; rất nghiêm trọng nếu có trên 2 người chết)
        - Địa điểm tai nạn
        - Thời tiết
        -Loại tai nạn
        Loại tai nạn là va chạm giữa gì để gây ra tai nạn
        Nếu không đề cập đến người bị thương , thương vong, tử vong thì số người chết và bị thương là 0
        Nếu không có thông tin cho cột nào, hãy để trống.
        Nếu không nhắc tới thời tiết thì mặc định là Không biết
        Hãy đảm bảo rằng câu trả lời của bạn rõ ràng và dễ hiểu.
        Nếu không tìm được gì hãy điền ngẫu nhiên nhé !
        <|im_end|>
        <|im_start|user:
        Hãy cho biết số người chết, số người bị thương, mức độ nghiêm trọng, địa điểm, thời tiết, loại tai nạn tai nạn của văn bản sau {question}<|im_end|>
        """

        prompt = self.create_prompts(template)
        llm = self.load_llms()
        chain = self.summarize_prompts(llm, prompt)

        content = question

        resp = chain.invoke({'question': content})
        if isinstance(resp, dict) and 'text' in resp:
            response_text = resp['text']
        else:
            response_text = str(resp)
        pattern = {
            'numberOfDeaths': r'(?:Số người chết|Người chết):\s*(\d+)',
            'numberOfInjured': r'(?:Số người bị thương|Người bị thương):\s*(\d+)',
            'serious': r'Mức độ nghiêm trọng:\s*(.+)',
            'location_address': r'Địa điểm tai nạn:\s*(.+)',
        }
        result = {
            key: re.search(regex, response_text).group(1) if re.search(regex, response_text) else ''
            for key, regex in pattern.items()
        }

        return result


model = Model('model/vinallama-7b-chat_q5_0.gguf')
re= model.respone("""
hời sựBản tin TNGT 13/11: Xe con biến dạng sau tai nạn với xe tải, một người tử vongNguyễn Hoàn-Diễm Ly
                                 -Nguyễn HoànDiễm Ly13/11/2024, 19:33Tính đến 15h ngày 13/11, chúng tôi ghi nhận ít nhất 1 vụ tai nạn giao thông khiến 1 người tử vong và 1 vụ sập cầu khiến các phương tiện hư hỏng nặng.
         """)
print(re)