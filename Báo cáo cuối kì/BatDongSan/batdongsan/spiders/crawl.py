import scrapy
import re

class MyscraperSpider(scrapy.Spider):
    name = "myscraper"
    allowed_domains = ["batdongsan.vn"]
    start_urls = [f'https://batdongsan.vn/ban-nha/p{i}' for i in range(1, 5)]

    def parse(self, response):
        # Lấy dữ liệu bất động sản từ trang hiện tại
        dongsans = response.css("a.card-cm")
        for dongsan in dongsans:
            tieu_de = dongsan.css("div.card-content h3::text").get()
            tieu_de = tieu_de.replace('\n', '').replace('\t', '').strip() if tieu_de else None

            dia_diem = dongsan.css("div.description::text").get()
            dia_diem = dia_diem.replace('\n', '').strip() if dia_diem else None
            dia_diem = re.sub(r'\s+', ' ', dia_diem) if dia_diem else None
            if dia_diem:
                dia_diem_parts = dia_diem.split(',')
                quan = dia_diem_parts[0].strip()
                thanh_pho = dia_diem_parts[1].strip()
            else:
                quan = None
                thanh_pho = None

            dien_tich = dongsan.css("div.description-tag div.description-item:nth-of-type(1)::text").get()
            dien_tich = dien_tich.replace('\n', '').replace('\t', '').strip() if dien_tich else None

            gia = dongsan.css("div.price::text").get()
            gia = gia.replace('\n', '').replace('\t', '').strip() if gia else None

            # Lấy liên kết đến trang chi tiết
            chi_tiet_link = response.urljoin(dongsan.css("a::attr(href)").get())

            # Điều hướng đến trang chi tiết
            yield scrapy.Request(url=chi_tiet_link, callback=self.parse_details, meta={
                'tieu_de': tieu_de,
                'quan': quan,
                'thanh_pho': thanh_pho,
                'dien_tich': dien_tich,
                'gia': gia,
                'chi_tiet_link': chi_tiet_link
            })

    def parse_details(self, response):
        # Lấy các cột từ trang chi tiết bất động sản
        phap_ly = response.xpath('//div[@class="col"]//div[@class="line"]//div[@class="line-label" and contains(., "Pháp lý")]/following-sibling::div[@class="line-text"]/text()').get()
        phap_ly = phap_ly.replace('\n', '').strip() if phap_ly else None

        so_phong_ngu = response.xpath('//div[@class="col"]//div[@class="line"]//div[@class="line-label" and contains(., "Số phòng ngủ")]/following-sibling::div[@class="line-text"]/text()').get()
        so_phong_ngu = so_phong_ngu.replace('\n', '').strip() if so_phong_ngu else None

        so_phong_toilet = response.xpath('//div[@class="col"]//div[@class="line"]//div[@class="line-label" and contains(., "Số toilet")]/following-sibling::div[@class="line-text"]/text()').get()
        so_phong_toilet = so_phong_toilet.replace('\n', '').strip() if so_phong_toilet else None

        ngay_dang = response.xpath('//div[@class="col"][div[@class="label" and contains(text(), "Ngày đăng")]]/div[@class="value"]/text()').get()
        ngay_dang = ngay_dang.replace('\n', '').replace('\t', '').strip() if ngay_dang else None

        ma_tin = response.xpath('//div[@class="col"][div[@class="label" and contains(text(), "Mã tin")]]/div[@class="value"]/text()').get()
        ma_tin = ma_tin.replace('\n', '').replace('\t', '').strip() if ma_tin else None

        duong = response.css("div.slide-description.col-md-12 div.footer::text").get()
        duong = duong.replace('\n','').replace('\t','').strip() if duong else None
        duong = re.sub(r'\s+', ' ', duong) if duong else None
        if duong:
            duong = duong.replace('Đường', '').strip()
            duong_parts = duong.split(',')
            duong = duong_parts[0].strip()

        mo_ta = response.css('div#more1::text').get()
        mo_ta = mo_ta.replace('\n', '').replace('\t', '').strip() if mo_ta else None

        # Kết hợp dữ liệu với tên sản phẩm và giá từ meta
        tieu_de = response.meta.get('tieu_de')
        quan = response.meta.get('quan')
        thanh_pho = response.meta.get('thanh_pho')
        dien_tich = response.meta.get('dien_tich')
        gia = response.meta.get('gia')
        chi_tiet_link = response.meta.get('chi_tiet_link')

        yield {
            'Mã_tin': ma_tin,
            'Tiêu_đề': tieu_de,
            'Đường': duong,
            'Quận': quan,
            'Thành_phố': thanh_pho,
            'Diện_tích': dien_tich,
            'Giá': gia,
            'Pháp_lý': phap_ly,
            'Số_phòng_ngủ': so_phong_ngu,
            'Số_phòng_toilet': so_phong_toilet,
            'Ngày_đăng': ngay_dang,
            'Mô_tả': mo_ta,
            'chi_tiet_link': chi_tiet_link
        }
